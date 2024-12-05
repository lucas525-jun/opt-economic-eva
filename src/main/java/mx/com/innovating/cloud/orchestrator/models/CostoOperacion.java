package mx.com.innovating.cloud.orchestrator.models;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class CostoOperacion {

	private Integer idcostooperacion;
	private String costooperacion;
	private String anio;
	private Double gasto;
}
