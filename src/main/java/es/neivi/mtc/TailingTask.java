/**

 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package es.neivi.mtc;

import java.util.concurrent.TimeUnit;

import org.bson.Document;
import org.bson.types.ObjectId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.CursorType;
import com.mongodb.MongoQueryException;
import com.mongodb.MongoSocketException;
import com.mongodb.ReadPreference;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;

import es.neivi.mtc.configuration.MTCConfiguration;
import es.neivi.mtc.configuration.MTCPersistentTrackingConfiguration;
import es.neivi.mtc.exceptions.CappedCollectionRequiredException;
import es.neivi.mtc.exceptions.DocumentHandlerRequiredException;
import es.neivi.mtc.exceptions.MTCException;
import es.neivi.mtc.exceptions.MTCExecutionException;
import es.neivi.mtc.exceptions.NotStartedException;

/**
 * Task responsible for fetching the events from the events (capped) collection.
 */
public class TailingTask implements Runnable {

	private static final Logger LOG = LoggerFactory
			.getLogger(TailingTask.class);

	private DocumentHandler documentHandler;
	private MTCConfiguration configuration;
	private ServiceStatus status = ServiceStatus.STOPPED;

	/**
	 * Collection storing events being published by systems interacting. Should
	 * be capped, have a size, max.
	 */
	// TODO: WiredTIGER StorageEngine(Concurrent Writes-READS). No
	// compression(capped, size is fixed)
	private MongoCollection<Document> eventsCollection;

	/*
	 * PERSISTENT TRACKER.
	 * 
	 * If proper persistent tracking configuration is set -> tracker != null &&
	 * cursorRegenerationDelay != 0
	 */

	private PersistentTrackingManager tracker;
	private long cursorRegenerationDelay;
	private ObjectId lastTrackedId = null;

	public TailingTask(MTCConfiguration configuration) {

		this.configuration = configuration;
		MongoDatabase mongoDatabase = configuration.getMongoDatabase();
		String collectionName = configuration.getCollection();
		eventsCollection = mongoDatabase.getCollection(collectionName);

		// Check eventsCollection is a capped collection...
		final Document collStatsCommand = new Document("collStats",
				collectionName);
		Boolean isCapped = mongoDatabase.runCommand(collStatsCommand,
				ReadPreference.primary()).getBoolean("capped");
		if (!isCapped) {
			throw new CappedCollectionRequiredException(
					"Tailable cursors are only compatible with capped collections, and collection "
							+ collectionName + " is not capped.");
		}

		// Persistent TRACKING ENABLED? If enabled tracker != null &&
		// cursorRegenerationDelay != 0
		if (configuration.isPersistentTrackingEnable()) {
			tracker = new PersistentTrackingManager(configuration);
			cursorRegenerationDelay = getConfiguration()
					.getPersistentTrackingConfiguration()
					.getCursorRegenerationDelay();
			if (cursorRegenerationDelay == 0) {
				cursorRegenerationDelay = MTCPersistentTrackingConfiguration.DEFAULT_CURSOR_REGENERATION_DELAY;
			}
		}
	}

	/**
	 * Builds a tailable & awaitdata cursor to fetch events from the events
	 * collection.
	 */
	public MongoCursor<Document> buildCursor() {

		if (lastTrackedId == null) {
			return eventsCollection.find().sort(new Document("$natural", 1))
					.cursorType(CursorType.TailableAwait).iterator();
		} else {

			// we know we processed the event with "_id": lastTrackedId
			// We are interested in the first event with id greater than
			// lastTrackedId
			return eventsCollection

			.find(Filters.gt("_id", lastTrackedId))

			.sort(new Document("$natural", 1))
					.cursorType(CursorType.TailableAwait).iterator();
		}
	}

	/**
	 * TAILING TASK:
	 * 
	 * 1. Builds a cursor 2. Fetch documents from that cursor till cursor closed
	 * o documentHandler changes its state
	 * 
	 * 
	 */
	@Override
	public void run() {

		try {

			// Check start was called
			if (!getStatus().equals(ServiceStatus.STARTED))
				throw new MTCExecutionException(
						"Trying to RUN a non started task. Please call start() method before running the tailing task. ");

			while (true) {

				// Work with cursor until lost or
				// documentHandler changes its state to not started.

				// hasNext throws IllegalStateException when cursor is closed
				// (not by documentHandler)
				MongoCursor<Document> cursor = buildCursor();
				if (cursor.hasNext()) {

					// throws ChangedStateToNotStarted
					iterateCursor(cursor);

					// Cursor was LOST

					// wait to regenerate another cursor if configured so
					applyDelayToGenerateCursor();
				} else {

					// hasNext returned with no data
					LOG.debug("Cursor returned no data");
					if (cursor != null)
						cursor.close();
				}

			} // while(keepRunning) block

		} catch (IllegalStateException e) {

			// hasNext() throws IllegalStateException when cursor is close by
			// other thread.
			LOG.info("+ MONGOESB: Cursor was closed");

			// STOP or Suspend

		} catch (NotStartedException e) {
			// Consumer changed its state
			LOG.info("+ MONGOESB: Consumer changed its state");
			// STOP or Suspend

			// } catch (MongoException e) {
			// LOG.info("+ MONGOESB: MongoException - STOP EXECUTION - {}",
			// e.toString());
			// throw new CamelMongoMBException(e);
			// } catch (RuntimeException e) {
			// throw new CamelMongoMBException(e);
		} finally {
			LOG.info("+ MONGOESB - STOP TAILING TASK");
		}

	} // run

	/**
	 * Cursor LOGIC. A cursor is built and can be iterated until lost or until
	 * documentHandler changes its state to a not started state. throws
	 * CamelMongoMBConsumerNotStarted to sign a change in documentHandler
	 * states.
	 * 
	 * @throws NotStartedException
	 *             to signal documentHandler state changed from started
	 */
	private void iterateCursor(final MongoCursor<Document> cursor) {

		// stores the id of the last event fetched by this cursor...
		ObjectId lastProcessedId = null;

		try {

			while (true) {

				// Is there a new document to be processed?
				Document next = cursor.tryNext();

				if (next == null) {

					// It is likely we come from a burst of processing ...

					// This is a chance to persist last processed
					// id...go for it

					if (tracker != null && lastProcessedId != null) {

						tracker.persistLastTrackedEventId(lastProcessedId);
						lastTrackedId = lastProcessedId;
					}

					// Wait for a new event to be processed
					if (!cursor.hasNext()) {
						LOG.debug("INNER has NEXT returned no data");
						if (cursor != null)
							cursor.close();
					}

				} else {

					// There is an event to be processed

					try {
						documentHandler.handleDocument(next);
						lastProcessedId = next.getObjectId("_id");
					} catch (Exception e) {
					}
				}

				// Check whether to keep execution
				if (getStatus().equals(ServiceStatus.STOPPED))
					throw new NotStartedException(
							"Cursor Changed its state to not started");
			} // while

		} catch (MongoSocketException e) {
			// The cursor was closed
			LOG.error("\n\nMONGOESB - NETWORK problems: Server Address: {}", e
					.getServerAddress().toString());

			// Not recoverable. Do not regenerate the cursor
			throw new MTCException(String.format(
					"Network Problemns detected. Server address: %s", e
							.getServerAddress().toString()));
		} catch (MongoQueryException e) {
			// MongoCursorNotFoundException
			// The cursor was closed
			// Recoverable: Do regenerate the cursor
			LOG.info("Cursor {} has been closed.");
		} catch (IllegalStateException e) {
			// .hasNext(): Cursor was closed by other THREAD (documentHandler
			// cleaningup)?)
			// Recoverable. Do regenerate the cursor.
			LOG.info("Cursor being iterated was closed\n{}", e.toString());
		} catch (NotStartedException e) {
			// Not recoverable: Do not regenerate the cursor.
			throw e;
		} finally {

			// persist tracking state
			if (tracker != null && lastProcessedId != null) {
				tracker.persistLastTrackedEventId(lastProcessedId);
				lastTrackedId = lastProcessedId;
			}

			// Cleanup resources.
			if (cursor != null)
				cursor.close();
		}
	}

	private void applyDelayToGenerateCursor() {

		if (cursorRegenerationDelay != 0) {
			try {
				TimeUnit.MILLISECONDS.sleep(cursorRegenerationDelay);
			} catch (InterruptedException e) {
				LOG.error("Thread was interrupted", e);
				Thread.currentThread().interrupt();
			}
		}
	}

	public DocumentHandler getDocumentHandler() {
		return documentHandler;
	}

	public MTCConfiguration getConfiguration() {
		return configuration;
	}

	public ServiceStatus getStatus() {
		return status;
	}

	public void start() {

		// Prestart logic:

		// 1. Check a document handler is set
		if (documentHandler == null)
			throw new DocumentHandlerRequiredException(
					"A documentHandler is REQUIRED in order to consume documents");

		// 2. fetch lastTrackedId if persistent tracking enabled
		MTCConfiguration configuration = getConfiguration();
		if (configuration.isPersistentTrackingEnable()) {
			lastTrackedId = tracker.fetchLastTrackedEventId();
		}

		// mark as started
		status = ServiceStatus.STARTED;
	}

	public void stop() {
		status = ServiceStatus.STOPPED;
	}

	public void setDocumentHandler(DocumentHandler documentHandler) {
		this.documentHandler = documentHandler;
	}
}
