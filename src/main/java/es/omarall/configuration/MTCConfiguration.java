package es.omarall.configuration;

import com.mongodb.MongoClient;
import com.mongodb.client.MongoDatabase;

import es.omarall.mtc.exceptions.InvalidMTCConfiguration;

/**
 * Contains all the data needed for exchange interaction.
 */
public class MTCConfiguration {

	/**
	 * A database connection with internal pooling.
	 */
	private MongoClient mongoClient;

	/**
	 * Holds the name provided for the database containing the collection that
	 * this component is bound to. Eventually, there will be a collection for
	 * persistent tracking purposes. Allowing the consumer tasks to fetch the
	 * last event they processed.
	 */
	private String database;

	/**
	 * Name provided for the collection that store all the documents that are
	 * going to be tailable consumed. This collections IS a capped collection.
	 */
	private String collection;

	/**
	 * Eventually, contains the data needed in order to implement a persistent
	 * tracking system. If this value is null, no tracking information is
	 * provided, meaning that persistent tracking is going to be disable. It
	 * tailing task is stop, when started again it will have no memory of the
	 * last processed documents
	 */
	private MTCPersistentTrackingConfiguration persistentTrackingConfiguration;

	/**
	 * MongoDatabase instance associated to collection named as database.
	 */
	private MongoDatabase mongoDatabase;

	public MongoClient getMongoClient() {
		return mongoClient;
	}

	public void setMongoClient(MongoClient mongoClient) {
		this.mongoClient = mongoClient;
	}

	public String getDatabase() {
		return database;
	}

	public void setDatabase(String database) {
		this.database = database;
	}

	public String getCollection() {
		return collection;
	}

	public void setCollection(String collection) {
		this.collection = collection;
	}

	public MongoDatabase getMongoDatabase() {
		if (mongoDatabase == null) {
			mongoDatabase = getMongoClient().getDatabase(getDatabase());
		}
		return mongoDatabase;
	}

	public void setMongoDatabase(MongoDatabase mongoDatabase) {
		this.mongoDatabase = mongoDatabase;
	}

	public MTCPersistentTrackingConfiguration getPersistentTrackingConfiguration() {
		return persistentTrackingConfiguration;
	}

	public void setPersistentTrackingConfiguration(MTCPersistentTrackingConfiguration persistentTrackingConfiguration) {
		this.persistentTrackingConfiguration = persistentTrackingConfiguration;
	}

	public boolean isPersistentTrackingEnable() {
		return (persistentTrackingConfiguration != null
				&& (!persistentTrackingConfiguration.getConsumerId().isEmpty()));
	}

	public void isValid() {
		if (mongoClient == null
				|| (persistentTrackingConfiguration != null && (persistentTrackingConfiguration.getConsumerId() == null
						|| persistentTrackingConfiguration.getConsumerId().isEmpty()))) {
			String m = "Invalid MTCConfiguration. Please check your URI. Remember you need MongoClient instance, and if persistent tracking configuration enable you need to specify a nonempty consumer task id";
			throw new InvalidMTCConfiguration(m);
		}
	}

	@Override
	public String toString() {
		return "MTCConfiguration [database=" + database + ", collection=" + collection
				+ ", persistentTrackingConfiguration=" + persistentTrackingConfiguration + "]";
	}
}
