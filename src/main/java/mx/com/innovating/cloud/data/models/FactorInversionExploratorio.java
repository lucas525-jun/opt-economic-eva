package mx.com.innovating.cloud.data.models;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class FactorInversionExploratorio {

	private String claveobjetivo;
	private String oportunidad;
	private Integer idinvexploratorio;
	private Integer idoportunidadobjetivo;
	private Integer idtipovalor;
	private Double infraestructura;
	private Double perforacion;
	private Double terminacion;
	private Double manoobrapozos;
	private Integer idversion;

}



