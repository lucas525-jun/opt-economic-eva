package mx.com.innovating.cloud.data.models;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class InformacionInversion {

    private String claveobjetivo;
    private String oportunidad;
    private Integer idotrosdatos;
    private Integer idoportunidadobjetivo;
    private Integer idtipovalor;
    private Double plataformadesarrollo;
    private Double lineadedescarga;
    private Double estacioncompresion;
    private Double ducto;
    private Double bateria;
    private Double arbolessubmarinos;
    private Double manifolds;
    private Double risers;
    private Double sistemasdecontrol;
    private Double cubiertadeproces;
    private Double buquetanquecompra;
    private Double buquetanquerenta;
    private Integer idversion;

}
