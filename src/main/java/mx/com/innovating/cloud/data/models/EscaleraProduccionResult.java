package mx.com.innovating.cloud.data.models;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import java.util.Date;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class EscaleraProduccionResult {
    private Integer vidconsecutivo;
    private Integer vvidsecuencia;
    private Integer vidpozo;
    private String vpozo;
    private Double vperfil;
    private Integer vdiasmes;
    private Integer vvidoportunidadobjetivo;
    private String vanio;
    private Integer vvmes;
    private Double vproduccion;
    private Date vfecha;
}
