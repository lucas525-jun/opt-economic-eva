package mx.com.innovating.cloud.orchestrator.models;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import java.math.BigDecimal;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class FactorCalculoForMulti {
    private Boolean isMulti;
    private BigDecimal sumPce = BigDecimal.ZERO;
    private BigDecimal sumFc_aceite = BigDecimal.ZERO;
    private BigDecimal sumFc_gas = BigDecimal.ZERO;
    private BigDecimal sumFc_condensado = BigDecimal.ZERO;
}