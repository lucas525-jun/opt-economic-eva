package mx.com.innovating.cloud.data.models;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class ProduccionPozos {

	private Integer cvidoportunidadobjetivo;
	private Integer cvidoportunidad;
	private String cvoportunidad;
	private String cvclaveoportunidad;
	private String cvclaveobjetivo;
	private Double cvprodacumulada;
	private Double cvnumpozo;

}
