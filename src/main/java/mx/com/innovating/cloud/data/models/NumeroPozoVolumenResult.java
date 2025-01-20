package mx.com.innovating.cloud.data.models;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class NumeroPozoVolumenResult {
    private Integer vidoportunidadobjetivo;
    private Integer vpozoporvolumen;
    private Double veur;
}