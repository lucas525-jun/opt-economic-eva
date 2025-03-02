package mx.com.innovating.cloud.data.models;

import java.util.Date;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class EscaleraProduccionMulti {
	private Integer idconsecutivo;
	private Integer idsecuencia;
	private Integer idpozo;
	private String pozo;
	private Double perfil;
	private Integer diasmes;
	private Integer idoportunidadobjetivo;
	private String anio;
	private Integer mes;
	private Double produccion;
	private Double prodaceite;
	private Double prodgas;
	private Double prodcondensado;
	private Date fecha;
}