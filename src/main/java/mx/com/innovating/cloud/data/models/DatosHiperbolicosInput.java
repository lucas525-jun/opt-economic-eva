package mx.com.innovating.cloud.data.models;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Date;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class DatosHiperbolicosInput {
    private Integer idoportunidadobjetivo;
    private Integer idoportunidad;
    private String oportunidad;
    private String claveoportunidad;
    private String claveobjetivo;
    private String fecha;
    private Integer meses;
    private Double hiperbolico;
    private Double gastoInicial;
    private Double b;
    private Double di;
    private Double rga;
    private Double rsiMedio;
    private Double frcMedio;
    private Double dimen;
    private String anio;
    private Integer dias;
    private Integer idTipoHidrocarburo;
    private String hidrocarburo;
    private Integer idVersion;
}