package com.pemex.pep.seemop.orchestrator.models;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;

@Data
@NoArgsConstructor
@JsonInclude(JsonInclude.Include.NON_NULL)
public class FactorCalculoForMulti {
    private Boolean isMulti = false;
    
    private List<FactorEntry> entries = new ArrayList<>();
    
    private BigDecimal sumPce = BigDecimal.ZERO;
    private BigDecimal sumFc_aceite = BigDecimal.ZERO;
    private BigDecimal sumFc_gas = BigDecimal.ZERO;
    private BigDecimal sumFc_condensado = BigDecimal.ZERO;
    
    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static class FactorEntry {
        private Double pce;
        private Double fc_aceite;
        private Double fc_gas;
        private Double fc_condensado;
    }

    public void addEntry(double pce, Double fc_aceite, Double fc_gas, Double fc_condensado) {
        FactorEntry entry = new FactorEntry(
            pce,
            fc_aceite,
            fc_gas,
            fc_condensado
        );
        
        if (entries == null) {
            entries = new ArrayList<>();
        }
        
        entries.add(entry);
    }
}