package mx.com.innovating.cloud.orchestrator.models;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class EvaluacionEconomica {

	private String anio;
	private Pozo pozosProduccion;
	private ProduccionDiariaPromedio produccionDiariaPromedio;
	private Ingresos ingresos;
	private Inversiones inversiones;
	private Costos costos;
	private FlujoContable flujoContable;

}
