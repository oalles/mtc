package es.omarall.mtc.exceptions;

public class DocumentHandlerRequiredException extends RuntimeException {

  public DocumentHandlerRequiredException() {

  }

  public DocumentHandlerRequiredException(String message) {
    super(message);

  }

  public DocumentHandlerRequiredException(Throwable cause) {
    super(cause);

  }

  public DocumentHandlerRequiredException(String message, Throwable cause) {
    super(message, cause);

  }

  public DocumentHandlerRequiredException(String message, Throwable cause,
      boolean enableSuppression, boolean writableStackTrace) {
    super(message, cause, enableSuppression, writableStackTrace);

  }

}
