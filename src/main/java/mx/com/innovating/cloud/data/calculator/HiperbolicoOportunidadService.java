package mx.com.innovating.cloud.data.service;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import lombok.extern.slf4j.Slf4j;
import mx.com.innovating.cloud.data.models.DatosHiperbolicosInput;
import mx.com.innovating.cloud.data.models.HiperbolicoOportunidadResult;
import mx.com.innovating.cloud.data.calculator.HiperbolicoOportunidadQueryService;
import mx.com.innovating.cloud.data.calculator.HiperbolicoCalculator;

import java.time.LocalDate;
import java.time.YearMonth;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.Map;
import jakarta.transaction.Transactional;

@Slf4j
@ApplicationScoped
public class HiperbolicoOportunidadService {

        @Inject
        HiperbolicoOportunidadQueryService queryService;

        @Inject
        HiperbolicoCalculatorService calculatorService;

        public List<HiperbolicoOportunidadResult> calculateHiperbolicoOportunidad(
                        Integer pnidversion,
                        Integer pnoportunidadobjetivo,
                        Integer pntipovalor,
                        Integer pnmeses) {

                try {
                        List<DatosHiperbolicosInput> datosIniciales = queryService.getDatosHiperbolicos(
                                        pnidversion,
                                        pnoportunidadobjetivo,
                                        pntipovalor,
                                        pnmeses);

                        if (datosIniciales.isEmpty()) {
                                log.warn("No data found for version: {}, oportunidad: {}, tipo: {}",
                                                pnidversion, pnoportunidadobjetivo, pntipovalor);
                                return new ArrayList<>();
                        }

                        DatosHiperbolicosInput datos = datosIniciales.get(0);
                        LocalDate startDate = LocalDate.parse(datos.getFecha());

                        // Calculate results using the calculator service
                        List<HiperbolicoOportunidadResult> results = calculatorService
                                        .calculateMonthlyResults(datos, pnmeses, startDate);

                        // Sort results as per original SPP
                        return results.stream()
                                        .sorted(Comparator
                                                        .comparing(HiperbolicoOportunidadResult::getVanio)
                                                        .thenComparing(HiperbolicoOportunidadResult::getVmes)
                                                        .thenComparing(HiperbolicoOportunidadResult::getVidsecuencia))
                                        .collect(Collectors.toList());

                } catch (Exception e) {
                        log.error("Error calculating hiperbólico oportunidad", e);
                        throw new RuntimeException("Error en cálculo hiperbólico", e);
                }
        }
}