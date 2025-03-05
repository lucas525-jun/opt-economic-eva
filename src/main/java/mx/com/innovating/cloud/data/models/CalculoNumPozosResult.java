package mx.com.innovating.cloud.data.models;

import lombok.*;

@Data
@Getter
@Setter
@AllArgsConstructor
@NoArgsConstructor
public class CalculoNumPozosResult {

    private Double nPozos;
    private String tipoCalculo;

}
