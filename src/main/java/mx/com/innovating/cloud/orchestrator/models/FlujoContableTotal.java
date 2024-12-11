package mx.com.innovating.cloud.orchestrator.models;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class FlujoContableTotal {

	private Double vpn;
	private Double tir;

	private Double flujosDescontadosEfectivoTotal;
	private Double valorPresenteInversion;
	private Double valorPresenteEgresos;
	private Double valorPresenteIngresos;
	private Double valorPresenteCostos;

	private Double reporte708;
	private Double reporte721;
	private Double reporte722;
	private Double reporte723;

	private Double utilidadBpce;
	private Double relacionCostoBeneficio;
	private Double costoDescubrimiento720;
	private Double costoOperacion;

}
