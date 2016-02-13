package es.omarall.mtc.exceptions;

/**
 * The configuration provided for the MTC component is INVALID.
 */
public class InvalidMTCConfiguration extends MTCException {

	public InvalidMTCConfiguration() {
		super();
	}

	public InvalidMTCConfiguration(String message, Throwable cause,
			boolean enableSuppression, boolean writableStackTrace) {
		super(message, cause, enableSuppression, writableStackTrace);
	}

	public InvalidMTCConfiguration(String message, Throwable cause) {
		super(message, cause);
	}

	public InvalidMTCConfiguration(String message) {
		super(message);
	}

	public InvalidMTCConfiguration(Throwable cause) {
		super(cause);
	}
}
