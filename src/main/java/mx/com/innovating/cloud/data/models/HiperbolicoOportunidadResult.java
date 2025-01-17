package mx.com.innovating.cloud.data.models;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class HiperbolicoOportunidadResult {
    private Integer vidsecuencia;
    private Integer vanio;
    private Integer vmes;
    private Integer vidtipohidrocarburo;
    private String vhidrocarburo;
    private Integer vidoportunidadobjetivo;
    private Integer vidoportunidad;
    private String voportunidad;
    private String vclaveoportunidad;
    private String vclaveobjetivo;
    private Double vhiperbolico;
    private Double vporcentaje;
    private Double vporcentajeajustado;
    private Double vhiperbolicoajustado;
    private Integer vdiasmes;
    private Double vproduccion;
    private Double vgasto;
    private Double vaceite;
    private Double vgas;
    private Double vcondensado;
    private Double vpmbpce;
}
