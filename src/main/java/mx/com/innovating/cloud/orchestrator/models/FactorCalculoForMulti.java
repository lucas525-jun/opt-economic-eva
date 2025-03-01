package mx.com.innovating.cloud.orchestrator.models;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class FactorCalculoForMulti {
    private Boolean isMulti;
    private Double sumPce;
    private Double sumFc_aceite;
    private Double sumFc_gas;
    private Double sumFc_condensado;
}