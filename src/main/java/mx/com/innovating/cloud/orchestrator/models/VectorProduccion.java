package mx.com.innovating.cloud.orchestrator.models;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class VectorProduccion {

	private String oportunidad;
	private String objetivo;
	private Integer idoportunidadobjetivo;
	private String anio;
	private Double totalmes;
	private Double totalanual;

}
