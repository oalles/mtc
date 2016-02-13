package es.omarall.mtc;

/**
 * Represents the lifecycle of the tailing task and inform about its current state.
 */
public interface Service {

	public void start();

	public void stop();

	public ServiceStatus getStatus();
}
