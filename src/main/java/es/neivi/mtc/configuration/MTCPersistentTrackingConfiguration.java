package es.neivi.mtc.configuration;

/**
 * Contains all the information related to enable a working persistent tracking
 * system, being able to allow a consmer task to remember the last event it
 * processed.
 */
public class MTCPersistentTrackingConfiguration {

	public static final String TRACKER_COLLECTION_NAME = "tracker";
	public static final String LAST_TRACK_ID_FIELD = "last-tracked-id";
	public static final String CONSUMER_ID_FIELD = "consumer-task-id";
	public static final long DEFAULT_CURSOR_REGENERATION_DELAY = 1000;

	/**
	 * Consumer task identifier. It is the only required parameter in order to
	 * enable persistent tracking system.
	 */
	private String consumerId;

	private long cursorRegenerationDelay = 1000L;

	public String getConsumerId() {
		return consumerId;
	}

	public void setConsumerId(String consumerId) {
		this.consumerId = consumerId;
	}

	public long getCursorRegenerationDelay() {
		return cursorRegenerationDelay;
	}

	public void setCursorRegenerationDelay(long cursorRegenerationDelay) {
		this.cursorRegenerationDelay = cursorRegenerationDelay;
	}

	@Override
	public String toString() {
		return "MTCPersistentTrackingConfiguration [consumerId=" + consumerId
				+ ", cursorRegenerationDelay=" + cursorRegenerationDelay + "]";
	}
}