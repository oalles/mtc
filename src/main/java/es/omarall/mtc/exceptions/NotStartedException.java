package es.omarall.mtc.exceptions;

/**
 * Signs the MongoESBConsumer changed its state to not started, so it is not
 * intented to consume documents from events collection.
 */
public class NotStartedException extends MTCException {

	public NotStartedException() {
	}

	public NotStartedException(String message, Throwable cause,
			boolean enableSuppression, boolean writableStackTrace) {
		super(message, cause, enableSuppression, writableStackTrace);
	}

	public NotStartedException(String message, Throwable cause) {
		super(message, cause);
	}

	public NotStartedException(String message) {
		super(message);
	}

	public NotStartedException(Throwable cause) {
		super(cause);
	}
}
