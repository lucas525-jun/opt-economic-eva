package mx.com.innovating.cloud.data.models;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class PrecioHidrocarburo {

	private Integer idhidrocarburo;
	private Integer idoportunidadobjetivo;
	private String hidrocarburo;
	private String anioprecio;
	private Double precio;

}
