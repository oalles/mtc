package es.neivi.mtc.exceptions;

/**
 * Signs the MongoESBConsumer changed its state to not started, so it is not
 * intented to consume documents from events collection.
 */
public class NotStartedException extends MTCException {

	public NotStartedException() {
		// TODO Auto-generated constructor stub
	}

	public NotStartedException(String message, Throwable cause,
			boolean enableSuppression, boolean writableStackTrace) {
		super(message, cause, enableSuppression, writableStackTrace);
		// TODO Auto-generated constructor stub
	}

	public NotStartedException(String message, Throwable cause) {
		super(message, cause);
		// TODO Auto-generated constructor stub
	}

	public NotStartedException(String message) {
		super(message);
		// TODO Auto-generated constructor stub
	}

	public NotStartedException(Throwable cause) {
		super(cause);
		// TODO Auto-generated constructor stub
	}

}
