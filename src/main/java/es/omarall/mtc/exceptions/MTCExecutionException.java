package es.omarall.mtc.exceptions;

public class MTCExecutionException extends MTCException {

	public MTCExecutionException() {
	}

	public MTCExecutionException(String message, Throwable cause,
			boolean enableSuppression, boolean writableStackTrace) {
		super(message, cause, enableSuppression, writableStackTrace);
	}

	public MTCExecutionException(String message, Throwable cause) {
		super(message, cause);
	}

	public MTCExecutionException(String message) {
		super(message);
	}

	public MTCExecutionException(Throwable cause) {
		super(cause);
	}
}
