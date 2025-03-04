package mx.com.innovating.cloud.data.models;

import lombok.*;

@Data
@Getter
@Setter
@AllArgsConstructor
@NoArgsConstructor
public class CalculoNumPozosResult {

    private Integer nPozos;
    private String tipoCalculo;

}
