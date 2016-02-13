package es.omarall.mtc.exceptions;

public class MTCException extends RuntimeException {

	public MTCException() {
		super();
	}

	public MTCException(String message, Throwable cause,
			boolean enableSuppression, boolean writableStackTrace) {
		super(message, cause, enableSuppression, writableStackTrace);
	}

	public MTCException(String message, Throwable cause) {
		super(message, cause);
	}

	public MTCException(String message) {
		super(message);
	}

	public MTCException(Throwable cause) {
		super(cause);
	}
}
