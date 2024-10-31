package mx.com.innovating.cloud.orchestrator.exeptions;

public class RestClientExecutionException extends RuntimeException {

	private static final long serialVersionUID = 1L;

	public RestClientExecutionException(String errorMessage) {
		super(errorMessage);
	}

}
