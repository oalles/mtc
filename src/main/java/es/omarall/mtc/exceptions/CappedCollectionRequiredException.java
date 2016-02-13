package es.omarall.mtc.exceptions;

/**
 * Signal that a capped collection is requiered to be bound to this endpoint.
 */
public class CappedCollectionRequiredException extends MTCException {
	
	public CappedCollectionRequiredException() {
	}

	public CappedCollectionRequiredException(String message, Throwable cause,
			boolean enableSuppression, boolean writableStackTrace) {
		super(message, cause, enableSuppression, writableStackTrace);
	}

	public CappedCollectionRequiredException(String message, Throwable cause) {
		super(message, cause);
	}

	public CappedCollectionRequiredException(String message) {
		super(message);
	}

	public CappedCollectionRequiredException(Throwable cause) {
		super(cause);
	}
}
