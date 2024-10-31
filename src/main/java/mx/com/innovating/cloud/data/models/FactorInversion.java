package mx.com.innovating.cloud.data.models;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class FactorInversion {

	private Integer idhidrocarburo;
	private Integer idoportunidadobjetivo;
	private String hidrocarburo;
	private Double pce;
	private Double factorAceite;
	private Double factorGas;
	private Double factorCondensado;
	private String anioprecio;

}
