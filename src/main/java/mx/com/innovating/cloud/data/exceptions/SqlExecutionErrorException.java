package mx.com.innovating.cloud.data.exceptions;

public class SqlExecutionErrorException extends RuntimeException {

	private static final long serialVersionUID = 1L;

	public SqlExecutionErrorException(String errorMessage) {
		super(errorMessage);
	}

}
