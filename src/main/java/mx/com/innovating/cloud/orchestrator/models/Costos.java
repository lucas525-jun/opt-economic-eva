package mx.com.innovating.cloud.orchestrator.models;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class Costos {

	private Double total;

	private Double fijos;

	private Double variables;
	private Double administracionCorporativo;
	private Double comprasGas;
	private Double comprasInterorganismos;

	private Double jubilados;
	private Double manoObra;

	private Double materiales;
	private Double otrosConceptos;
	private Double reservaLaboral;
	private Double serviciosCorporativos;
	private Double serviciosGenerales;

}
