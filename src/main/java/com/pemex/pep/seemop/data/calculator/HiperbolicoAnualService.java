package com.pemex.pep.seemop.data.calculator;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

@ApplicationScoped
public class HiperbolicoAnualService {

        @Data
        @AllArgsConstructor
        @NoArgsConstructor
        public static class HiperbolicoAnualResult {
                private String panio;
                private String phidrocarburo;
                private String poportunidad;
                private Double pporcentajeajustado;
                private Double paceite;
                private Double pgas;
                private Double pcondensado;
                private Double ppmbpce;
                private Double fcaceite;
                private Double fcgas;
                private Double fccondensado;
        }

        @Data
        @AllArgsConstructor
        @NoArgsConstructor
        private static class HiperbolicoAnual {
                private String anio;
                private String oportunidad;
                private String hidrocarburo;
                private Double hiperbolico;
                private Double aceite;
                private Double gas;
                private Double condensado;
                private Double pmbpce;
                private Double fcaceite;
                private Double fcgas;
                private Double fccondensado;
        }

        @Inject
        HiperbolicoOportunidadService hiperbolicoOportunidadService;

        public List<HiperbolicoAnualResult> sppHiperbolicoAnual(Integer pnidversion,
                        Integer pnoportunidadobjetivo,
                        Integer pntipovalor,
                        Integer pnmeses) {

                // Create temporary data structure (equivalent to CREATE TEMP TABLE in SPP)
                List<HiperbolicoAnual> hiperbolicoanual = new ArrayList<>();

                // Get data from HiperbolicoOportunidad service
                List<HiperbolicoOportunidadService.HiperbolicoResult> oportunidadResults = hiperbolicoOportunidadService
                                .sppHiperbolicoOportunidad(pnidversion, pnoportunidadobjetivo, pntipovalor, pnmeses);

                // Group and calculate averages (equivalent to INSERT INTO with GROUP BY in SPP)
                var groupedResults = oportunidadResults.stream()
                                .collect(Collectors.groupingBy(
                                                result -> String.format("%s_%s_%s",
                                                                result.getVanio(),
                                                                result.getVoportunidad(),
                                                                result.getVhidrocarburo()),
                                                Collectors.collectingAndThen(
                                                                Collectors.toList(),
                                                                list -> {
                                                                        double avgHiperbolicoAjustado = list.stream()
                                                                                        .mapToDouble(HiperbolicoOportunidadService.HiperbolicoResult::getVhiperbolicoajustado)
                                                                                        .average()
                                                                                        .orElse(0.0);
                                                                        double avgAceite = list.stream()
                                                                                        .mapToDouble(HiperbolicoOportunidadService.HiperbolicoResult::getVaceite)
                                                                                        .average()
                                                                                        .orElse(0.0);
                                                                        double avgGas = list.stream()
                                                                                        .mapToDouble(HiperbolicoOportunidadService.HiperbolicoResult::getVgas)
                                                                                        .average()
                                                                                        .orElse(0.0);
                                                                        double avgCondensado = list.stream()
                                                                                        .mapToDouble(HiperbolicoOportunidadService.HiperbolicoResult::getVcondensado)
                                                                                        .average()
                                                                                        .orElse(0.0);
                                                                        double avgPmbpce = list.stream()
                                                                                        .mapToDouble(HiperbolicoOportunidadService.HiperbolicoResult::getVpmbpce)
                                                                                        .average()
                                                                                        .orElse(0.0);

                                                                        return new HiperbolicoAnual(
                                                                                        list.get(0).getVanio()
                                                                                                        .toString(),
                                                                                        list.get(0).getVhidrocarburo(),
                                                                                        list.get(0).getVoportunidad(),
                                                                                        avgHiperbolicoAjustado,
                                                                                        avgAceite,
                                                                                        avgGas,
                                                                                        avgCondensado,
                                                                                        avgPmbpce,
                                                                                        avgPmbpce != 0 ? avgAceite
                                                                                                        / avgPmbpce
                                                                                                        : 0.0,
                                                                                        avgPmbpce != 0 ? avgGas
                                                                                                        / avgPmbpce
                                                                                                        : 0.0,
                                                                                        avgPmbpce != 0 ? avgCondensado
                                                                                                        / avgPmbpce
                                                                                                        : 0.0);
                                                                })));

                // Convert grouped results to list
                hiperbolicoanual.addAll(groupedResults.values());

                // Convert to final result format
                List<HiperbolicoAnualResult> results = hiperbolicoanual.stream()
                                .map(hip -> new HiperbolicoAnualResult(
                                                hip.getAnio(),
                                                hip.getHidrocarburo(),
                                                hip.getOportunidad(),
                                                hip.getHiperbolico(), // This maps to pporcentajeajustado
                                                hip.getAceite(),
                                                hip.getGas(),
                                                hip.getCondensado(),
                                                hip.getPmbpce(),
                                                hip.getFcaceite(),
                                                hip.getFcgas(),
                                                hip.getFccondensado()))
                                .sorted((a, b) -> {
                                        int yearComp = Integer.compare(
                                                        Integer.parseInt(a.getPanio()),
                                                        Integer.parseInt(b.getPanio()));
                                        if (yearComp != 0)
                                                return yearComp;
                                        return a.getPoportunidad().compareTo(b.getPoportunidad());
                                })
                                .collect(Collectors.toList());

                // Clear temp data structure (equivalent to DROP TABLE in SPP)
                hiperbolicoanual.clear();

                return results;
        }
}