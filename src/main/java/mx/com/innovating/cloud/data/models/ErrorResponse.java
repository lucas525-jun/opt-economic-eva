package mx.com.innovating.cloud.data.models;

import java.io.Serializable;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class ErrorResponse implements Serializable {

	private static final long serialVersionUID = 1L;

	private String error;
	private String timestamp;

}
