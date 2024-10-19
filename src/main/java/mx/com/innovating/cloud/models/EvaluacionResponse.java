package mx.com.innovating.cloud.models;

import java.util.List;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class EvaluacionResponse {

	private InformacionOportunidad infoOportunidad;
	private List<EvaluacionEconomica> evaluacionEconomica;
	private FlujoContableTotal flujoContableTotal;

}
