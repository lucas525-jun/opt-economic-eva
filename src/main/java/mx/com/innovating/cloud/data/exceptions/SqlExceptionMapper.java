package mx.com.innovating.cloud.data.exceptions;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.ext.ExceptionMapper;
import jakarta.ws.rs.ext.Provider;
import mx.com.innovating.cloud.data.models.ErrorResponse;

@Provider
public class SqlExceptionMapper implements ExceptionMapper<SqlExecutionErrorException> {
	@Override
	public Response toResponse(SqlExecutionErrorException exception) {

		LocalDateTime now = LocalDateTime.now();
		DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
		String formattedNow = now.format(formatter);

		ErrorResponse err = new ErrorResponse();
		err.setError(exception.getMessage());
		err.setTimestamp(formattedNow);
		return Response.status(Response.Status.INTERNAL_SERVER_ERROR).entity(err).build();

	}

}
