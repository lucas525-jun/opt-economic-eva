package mx.com.innovating.cloud.data.models;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class PozoVolumenResult {
    private Integer cvidoportunidadobjetivo;
    private Integer cvidoportunidad;
    private String cvoportunidad;
    private String cvclaveOportunidad;
    private String cvclaveObjetivo;
    private Double cvprodAcumulada;
    private Double cvNumPozo;
}