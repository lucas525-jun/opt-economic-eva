package mx.com.innovating.cloud.orchestrator.models;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import mx.com.innovating.cloud.data.entities.InformacionOportunidad;

import java.util.List;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class EvaluacionResponse {
    private Double areamasignacion;
    private Double pceSimuladoOportunidad;
    private InformacionOportunidad infoOportunidad;
    private List<EvaluacionEconomica> evaluacionEconomica;
    private FlujoContableTotal flujoContableTotal;
    private FactorCalculo factorCalculo;
    private String fechaTermino;
    private int pozosTerminadosValue;
    private FactorCalculoForMulti factorCalculoForMulti;

    public EvaluacionResponse(Double areamasignacion,
            Double pceSimuladoOportunidad,
            InformacionOportunidad infoOportunidad,
            List<EvaluacionEconomica> evaluacionEconomica,
            FlujoContableTotal flujoContableTotal,
            FactorCalculo factorCalculo) {
        this(areamasignacion,
                pceSimuladoOportunidad,
                infoOportunidad,
                evaluacionEconomica,
                flujoContableTotal,
                factorCalculo,
                null,
                0, null);
    }
}