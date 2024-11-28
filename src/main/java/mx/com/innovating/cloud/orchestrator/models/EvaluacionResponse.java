package mx.com.innovating.cloud.orchestrator.models;

import java.util.List;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import mx.com.innovating.cloud.data.entities.InformacionOportunidad;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class EvaluacionResponse {

	private InformacionOportunidad infoOportunidad;
	private List<EvaluacionEconomica> evaluacionEconomica;
	private FlujoContableTotal flujoContableTotal;
	private  FactorCalculo factorCalculo;

}
