package mx.com.innovating.cloud.data.models;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class Oportunidades {

    private int idoportunidadobjetivo;
    private int idoportunidad;
    private String oportunidad;
    private String nombreVersion;

}
