package mx.com.innovating.cloud.orchestrator.models;

import java.math.BigDecimal;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class Pozo {

	private BigDecimal pozosActivos;
	private BigDecimal pozosPerforados;
	private BigDecimal pozosTerminados;

}
