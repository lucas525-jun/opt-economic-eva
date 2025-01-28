package mx.com.innovating.cloud.data.models;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Date;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class DeclinadaMes {
    private Integer idOportunidadObjetivo;
    private Integer idOportunidad;
    private String oportunidad;
    private String claveOportunidad;
    private String claveObjetivo;
    private String fecha;
    private Integer meses;
    private Integer idVersion;
    private Double lnDeclinada;
    private String anio;
    private Integer dias;
    private Double primDeclinacionOportunidad;
    private Double gastoInicial;
    private Double recurso;
    private Date mesInicio;
}