package es.neivi.mtc;

import org.bson.Document;
import org.bson.types.ObjectId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.model.IndexOptions;
import com.mongodb.client.model.UpdateOptions;

import es.neivi.mtc.configuration.MTCConfiguration;
import es.neivi.mtc.configuration.MTCPersistentTrackingConfiguration;

/**
 * In charg of tracking database which holds the information needed to be able
 * to start consuming again from an specified document. This instance persists
 * the id of the last tracked event for a given consumer and fetches the last
 * tracked event id from the db.
 * 
 * Tracker database has {_id | consumer-task-id | last-tracked_id} and has a
 * single field index {consumer-task-id: 1}
 */
public class PersistentTrackingManager {

	private static final Logger LOG = LoggerFactory
			.getLogger(PersistentTrackingManager.class);

	private final MTCConfiguration configuration;
	private MongoCollection<Document> trackerCollection;

	public PersistentTrackingManager(MTCConfiguration configuration) {

		if (!configuration.isPersistentTrackingEnable())
			throw new IllegalArgumentException(
					"Inconsistence: We expected a MongoESBConfiguration instance, with persistent configuration enabled");

		this.configuration = configuration;
		this.trackerCollection = configuration
				.getMongoDatabase()
				.getCollection(
						MTCPersistentTrackingConfiguration.TRACKER_COLLECTION_NAME);

		// Check if it has an INDEX on the field
		// MongoESBPersistentTrackingConfiguration.CONSUMER_ID_FIELD
		MongoCursor<Document> indexesCursor = this.trackerCollection
				.listIndexes().iterator();
		boolean indexExist = false;
		while (indexesCursor.hasNext()) {
			Document index = indexesCursor.next();
			Document key = (Document) index.get("key");
			if (key.containsKey(MTCPersistentTrackingConfiguration.CONSUMER_ID_FIELD)) {
				indexExist = true;
				break;
			}	
			// LOG.debug("Index: {}", index);
		}
		
		if (!indexExist) {

			// Option 1: BUILD INDEX
			IndexOptions indexOptions = new IndexOptions().unique(true);
			Document index = new Document(
					MTCPersistentTrackingConfiguration.CONSUMER_ID_FIELD, 1);
			this.trackerCollection.createIndex(index, indexOptions);
			LOG.info("+ MONGOESB - Index built: {}",
					MTCPersistentTrackingConfiguration.CONSUMER_ID_FIELD);

			// Option 2. STOP and NOTIFY
			// String m = String
			// .format("Collection: %s should have an index on %s field",
			// MongoESBPersistentTrackingConfiguration.TAILTRACKING_COLLECTION_NAME,
			// MongoESBPersistentTrackingConfiguration.CONSUMER_ID_FIELD);
			// LOG.error(m);
			// throw new MTCException(m);
		}

	}

	/**
	 * Insert the _id for a event processed by a consumerId in the persistent
	 * tracking collection
	 * 
	 * @throws com.mongodb.MongoWriteException
	 *             if the write failed due some other failure specific to the
	 *             update command
	 * @throws com.mongodb.MongoWriteConcernException
	 *             if the write failed due being unable to fulfil the write
	 *             concern
	 * @throws com.mongodb.MongoException
	 *             if the write failed due some other failure
	 * @throws java.lang.IllegalArgumentException
	 *             if a null argument was passed to the method
	 */
	public void persistLastTrackedEventId(final ObjectId processedEventId) {

		if (processedEventId == null) {
			String m = "A not null eventId was expected. This show some type of inconsistence in the application?";
			LOG.error(m);
			throw new IllegalArgumentException(m);
		}

		// san index on (CONSUMER-ID)is needed
		Document filter = new Document(
				MTCPersistentTrackingConfiguration.CONSUMER_ID_FIELD,
				configuration.getPersistentTrackingConfiguration()
						.getConsumerId());

		Document update = new Document("$set", new Document(
				MTCPersistentTrackingConfiguration.LAST_TRACK_ID_FIELD,
				processedEventId));

		// Throws RTE: MongoException, MongoWriteException,
		// MongoWriteConcernException

		trackerCollection.updateOne(filter, update,
				new UpdateOptions().upsert(true));

		LOG.debug("\n+ MongoESB - Last Event ID persisted: {}.\n",
				processedEventId);
	}

	/**
	 * get the last processed event id associated with the bound consumer task
	 * id.
	 * 
	 * @return null - no tracking information available for current consumer id.
	 * @return last processed event id for the bound consumer id.
	 * 
	 */
	public synchronized ObjectId fetchLastTrackedEventId() {

		// Needs an index on (CONSUMER-ID)
		// Filter by consumer-id
		Document filter = new Document(
				MTCPersistentTrackingConfiguration.CONSUMER_ID_FIELD,
				configuration.getPersistentTrackingConfiguration()
						.getConsumerId());

		// We get the last track record for this given consumer
		Document lastRecordByConsumer = trackerCollection.find(filter).first();

		if (lastRecordByConsumer != null) {

			return lastRecordByConsumer
					.getObjectId(MTCPersistentTrackingConfiguration.LAST_TRACK_ID_FIELD);
		}

		// none event id persisted for the cosumer id bound to this instance

		return null;
	}

}
