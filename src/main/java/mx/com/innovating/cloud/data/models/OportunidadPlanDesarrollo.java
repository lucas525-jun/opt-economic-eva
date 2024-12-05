package mx.com.innovating.cloud.data.models;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class OportunidadPlanDesarrollo {

    private Integer idoportunidad;
    private String oportunidad;
    private Integer idoportunidadobjetivo;
    private Integer idplanDesarrollo;
    private String nombreVersion;
    private Integer idinversion;
    private String tipoinversion;
    private Double duracion;
    private Integer idversion;

}