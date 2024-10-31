package mx.com.innovating.cloud.orchestrator.exeptions;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

import jakarta.ws.rs.core.Response;
import jakarta.ws.rs.ext.ExceptionMapper;
import jakarta.ws.rs.ext.Provider;
import mx.com.innovating.cloud.orchestrator.models.ErrorResponse;

@Provider
public class RestClientExceptionMapper implements ExceptionMapper<RestClientExecutionException> {

	@Override
	public Response toResponse(RestClientExecutionException exception) {

		LocalDateTime now = LocalDateTime.now();
		DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
		String formattedNow = now.format(formatter);

		ErrorResponse err = new ErrorResponse();
		err.setError(exception.getMessage());
		err.setTimestamp(formattedNow);
		return Response.status(Response.Status.INTERNAL_SERVER_ERROR).entity(err).build();

	}

}
