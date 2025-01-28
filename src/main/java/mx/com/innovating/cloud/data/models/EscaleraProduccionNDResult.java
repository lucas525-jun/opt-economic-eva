package mx.com.innovating.cloud.data.models;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import java.util.Date;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class EscaleraProduccionNDResult {
    private Integer vvidsecuencia;
    private Integer vidpozo;
    private String vpozo;
    private Integer vvidoportunidadobjetivo;
    private String vanio;
    private Double vproduccion;
    private Date vfecha;
}
