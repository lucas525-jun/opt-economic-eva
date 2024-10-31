package mx.com.innovating.cloud.data.exceptions;

public class SqlNotFoundException extends RuntimeException {

	private static final long serialVersionUID = 1L;

	public SqlNotFoundException(String errorMessage) {
		super(errorMessage);
	}

}
