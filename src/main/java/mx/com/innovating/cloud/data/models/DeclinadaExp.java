package mx.com.innovating.cloud.data.models;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import java.util.Date;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class DeclinadaExp {
    private Integer idTempExp;
    private Integer idOportunidadObjetivo;
    private Date fecha;
    private Integer fechaInicio;
    private Integer meses;
    private Integer nMes;
    private Integer idVersion;
    private Double lnDeclinada;
    private Double expDeclinada;
    private Integer anio;
    private Integer dias;
    private Integer diasMes;
    private Double primDeclinacionOportunidad;
    private Double gastoInicial;
    private Double produccion;
}