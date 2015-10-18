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
 * Task responsible for fetching the documents from the (capped) collection.
 */
public class TailingTask implements Runnable, Service {

	private static final Logger LOG = LoggerFactory
			.getLogger(TailingTask.class);

	private DocumentHandler documentHandler;
	private MTCConfiguration configuration;
	private ServiceStatus status = ServiceStatus.STOPPED;

	/**
	 * Collection storing documents being published by systems interacting.
	 * Should be capped, have a size, max.
	 */
	// TODO: WiredTIGER StorageEngine(Concurrent Writes-READS). No
	// compression(capped, size is fixed)
	private MongoCollection<Document> cappedCollection;

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

		// Check configuration is VALID
		configuration.isValid();
		LOG.debug("MTCConfiguration: VALID\n{}", configuration.toString());

		this.configuration = configuration;

		MongoDatabase mongoDatabase = configuration.getMongoDatabase();
		String collectionName = configuration.getCollection();
		cappedCollection = mongoDatabase.getCollection(collectionName);

		// Check cappedCollection is a capped collection...
		final Document collStatsCommand = new Document("collStats",
				collectionName);
		Boolean isCapped = mongoDatabase.runCommand(collStatsCommand,
				ReadPreference.primary()).getBoolean("capped");
		if (!isCapped) {
			throw new CappedCollectionRequiredException(
					"Tailable cursors are only compatible with capped collections, and collection "
							+ collectionName + " is not capped.");
		}
		
		// Is Capped.
		LOG.debug("Collection {} is CAPPED as expected", collectionName);

		// Persistent TRACKING ENABLED? If enabled tracker != null &&
		// cursorRegenerationDelay != 0
		if (configuration.isPersistentTrackingEnable()) {
			LOG.debug("Persistent tracking is ENABLED");
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
	 * Builds a tailable & awaitdata cursor to fetch documents from the
	 * documents collection.
	 * 
	 * @return
	 */
	public MongoCursor<Document> buildCursor() {

		if (lastTrackedId == null) {
			return cappedCollection.find().sort(new Document("$natural", 1))
					.cursorType(CursorType.TailableAwait).iterator();
		} else {

			// we know we processed the document with "_id": lastTrackedId
			// We are interested in the first document with id greater than
			// lastTrackedId
			return cappedCollection

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
				// "Await" for data
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
		} finally {
			LOG.info("+ MONGOESB - STOP TAILING TASK");
		}

	} // run

	/**
	 * Cursor LOGIC. A built cursor can be iterated until lost or until the
	 * state is changed to a no started state.
	 * 
	 * @throws NotStartedException
	 *             to signal state changed to a non started state
	 */
	private void iterateCursor(final MongoCursor<Document> cursor) {

		// stores the id of the last document fetched by THIS cursor...
		ObjectId lastProcessedId = null;

		try {

			while (true) {

				// Is there a new document to be processed?
				Document next = cursor.tryNext();

				if (next == null) {
					
					// No doc to be processed ...
					// It is likely we come from a burst of processing ...
					// This is a chance to persist last processed
					// id...go for it

					if (tracker != null && lastProcessedId != null) {

						tracker.persistLastTrackedEventId(lastProcessedId);
						lastTrackedId = lastProcessedId;
					}

					// Wait for a new document to be processed
					if (!cursor.hasNext()) {
						LOG.debug("INNER has NEXT returned no data");
						if (cursor != null)
							cursor.close();
					}

				} else {

					// There is a document to be processed
					try {

						documentHandler.handleDocument(next);
						lastProcessedId = next.getObjectId("_id");
					} catch (Exception e) {
						LOG.error("DocumentHandler raised an exception", e);
						// Notifiy but keep going
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
					.getServerAddress().toString(), e);

			// Not recoverable. Do not regenerate the cursor
			throw new MTCException(String.format(
					"Network Problemns detected. Server address: %s", e
							.getServerAddress().toString()));
		} catch (MongoQueryException e) {
			// MongoCursorNotFoundException
			// The cursor was closed
			// Recoverable: Do regenerate the cursor
			LOG.info("Cursor {} has been closed.", e);
		} catch (IllegalStateException e) {
			// .hasNext(): Cursor was closed by other THREAD (documentHandler
			// cleaningup)?)
			// Recoverable. Do regenerate the cursor.
			LOG.info("Cursor being iterated was closed", e);
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

	/**
	 * When a cursor was closed a delay can be set to wait for another cursor
	 * construction
	 */
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

	@Override
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

	@Override
	public void stop() {
		status = ServiceStatus.STOPPED;
	}

	public void setDocumentHandler(DocumentHandler documentHandler) {
		this.documentHandler = documentHandler;
	}
}
