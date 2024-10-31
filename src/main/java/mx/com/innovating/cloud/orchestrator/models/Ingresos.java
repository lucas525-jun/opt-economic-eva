package mx.com.innovating.cloud.orchestrator.models;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class Ingresos {

	private Double total;

	private Double aceiteExtraPesado;
	private Double aceitePesado;
	private Double aceiteLigero;
	private Double aceiteSuperLigero;

	private Double gasHumedo;
	private Double gasSeco;

	private Double condensado;

}
