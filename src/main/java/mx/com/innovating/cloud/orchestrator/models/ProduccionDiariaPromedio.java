package mx.com.innovating.cloud.orchestrator.models;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class ProduccionDiariaPromedio {

	private Double mbpce;

	private Double aceiteTotal;
	private Double aceiteExtraPesado;
	private Double aceitePesado;
	private Double aceiteLigero;
	private Double aceiteSuperLigero;

	private Double gasTotal;
	private Double gasHumedo;
	private Double gasSeco;

	private Double condensado;

}
