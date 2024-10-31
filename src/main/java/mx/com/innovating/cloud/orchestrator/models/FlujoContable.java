package mx.com.innovating.cloud.orchestrator.models;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class FlujoContable {

	private Double egresosTotales;
	private Double flujosNetosEfectivo;

	private Double flujosDescontadosEfectivo;
	private Double flujosDescontadosInversion;
	private Double flujosDescontadosEgresos;
	private Double flujosDescontadosIngresos;
	private Double flujosDescontadosCostos;

}
