package mx.com.innovating.cloud.data.calculator;

import io.quarkus.cache.CacheResult;
import io.quarkus.cache.CacheKey;
import io.quarkus.logging.Log;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.persistence.EntityManager;
import jakarta.persistence.Query;
import jakarta.transaction.Transactional;

import mx.com.innovating.cloud.data.models.VectorProduccion;
import mx.com.innovating.cloud.data.models.EscaleraProduccion;
import mx.com.innovating.cloud.data.models.EscaleraProduccion;
import mx.com.innovating.cloud.data.calculator.EscaleraProduccionService;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.DoubleSummaryStatistics;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

@ApplicationScoped
public class ProduccionAnualService {

        private static final BigDecimal THOUSAND = BigDecimal.valueOf(1000);
        private static final BigDecimal DAYS_IN_YEAR = BigDecimal.valueOf(365);

        @Inject
        EntityManager em;

        @Inject
        EscaleraProduccionService escaleraProduccionService;

        @Transactional
        @CacheResult(cacheName = "oportunidad-objetivo-cache")
        public List<Object[]> getOportunidadObjetivoData(@CacheKey int idoportunidadobjetivo) {
                String sql = """
                                    SELECT cv.oportunidad, cv.objetivo, cv.idoportunidadobjetivo
                                    FROM catalogo.datosoportunidadobjetivovw cv
                                    WHERE cv.idoportunidadobjetivo = :idoportunidadobjetivo
                                """;

                Query query = em.createNativeQuery(sql)
                                .setParameter("idoportunidadobjetivo", idoportunidadobjetivo);

                return query.getResultList();
        }

        @Transactional
        public List<VectorProduccion> calculateProduccionAnual(
                        int pnidversion,
                        int pnoportunidadobjetivo,
                        double pncuota,
                        double pndeclinada,
                        double pnpce,
                        double pnarea) {

                // Validate input parameters
                validateInputParameters(pnidversion, pnoportunidadobjetivo, pncuota, pndeclinada, pnpce, pnarea);

                // Get escalera produccion data
                List<EscaleraProduccion> escaleraResults = escaleraProduccionService
                                .calculateEscaleraProduccion(pnidversion, pnoportunidadobjetivo,
                                                pncuota, pndeclinada, pnpce, pnarea);

                // Log input parameters and results count for debugging
                logCalculationDetails(pnidversion, pnoportunidadobjetivo, escaleraResults);

                // Group production results with BigDecimal for precise calculations
                Map<ProductionKey, BigDecimal> groupedProduction = escaleraResults.stream()
                                .collect(Collectors.groupingBy(
                                                er -> new ProductionKey(
                                                                er.getIdoportunidadobjetivo(),
                                                                er.getAnio()),
                                                Collectors.reducing(
                                                                BigDecimal.ZERO,
                                                                er -> BigDecimal.valueOf(er.getProduccion()),
                                                                BigDecimal::add)));

                // Fetch catalog data
                List<Object[]> catalogData = getOportunidadObjetivoData(pnoportunidadobjetivo);

                // Transform to VectorProduccion with precise calculations
                return groupedProduction.entrySet().stream()
                                .map(entry -> {
                                        ProductionKey key = entry.getKey();
                                        BigDecimal sumProduccion = entry.getValue();

                                        // Precise monthly and annual calculations
                                        BigDecimal totalMes = sumProduccion.multiply(THOUSAND)
                                                        .setScale(10, RoundingMode.HALF_EVEN);
                                        BigDecimal totalAnual = totalMes.divide(DAYS_IN_YEAR, 10,
                                                        RoundingMode.HALF_EVEN);

                                        // Find matching catalog entry
                                        Optional<Object[]> catalogInfo = catalogData.stream()
                                                        .filter(row -> (Integer) row[2] == key.idoportunidadobjetivo)
                                                        .findFirst();

                                        return catalogInfo.map(info -> new VectorProduccion(
                                                        (String) info[0], // oportunidad
                                                        (String) info[1], // objetivo
                                                        key.idoportunidadobjetivo,
                                                        key.anio,
                                                        totalMes.doubleValue(), // ctotalmes
                                                        totalAnual.doubleValue() // ctotalanual
                                        )).orElse(null);
                                })
                                .filter(Objects::nonNull)
                                .sorted(Comparator.comparing(VectorProduccion::getAanio))
                                .collect(Collectors.toList());
        }

        // Input parameter validation
        private void validateInputParameters(
                        int pnidversion,
                        int pnoportunidadobjetivo,
                        double pncuota,
                        double pndeclinada,
                        double pnpce,
                        double pnarea) {

                if (pnidversion <= 0) {
                        throw new IllegalArgumentException("Invalid version ID: " + pnidversion);
                }
                if (pnoportunidadobjetivo <= 0) {
                        throw new IllegalArgumentException(
                                        "Invalid opportunity objective ID: " + pnoportunidadobjetivo);
                }
                if (pncuota < 0 || pndeclinada < 0 || pnpce < 0 || pnarea < 0) {
                        throw new IllegalArgumentException(
                                        "Negative values not allowed for quota, declined, PCE, or area");
                }
        }

        // Detailed logging for debugging
        private void logCalculationDetails(
                        int pnidversion,
                        int pnoportunidadobjetivo,
                        List<EscaleraProduccion> escaleraResults) {

                Log.infof("Calculation Parameters: " +
                                "Version ID: %d, " +
                                "Opportunity Objective ID: %d, " +
                                "Total Escalera Results: %d",
                                pnidversion,
                                pnoportunidadobjetivo,
                                escaleraResults.size());

                // Optional: More detailed logging if needed
                escaleraResults.stream()
                                .limit(10) // Log first 10 results to avoid overwhelming logs
                                .forEach(result -> Log.debugf(
                                                "Escalera Result - Opportunity: %d, Year: %s, Production: %f",
                                                result.getIdoportunidadobjetivo(),
                                                result.getAnio(),
                                                result.getProduccion()));
        }

        // Helper class for grouping production results
        private static class ProductionKey {
                final int idoportunidadobjetivo;
                final String anio;

                ProductionKey(int idoportunidadobjetivo, String anio) {
                        this.idoportunidadobjetivo = idoportunidadobjetivo;
                        this.anio = anio;
                }

                @Override
                public boolean equals(Object o) {
                        if (this == o)
                                return true;
                        if (o == null || getClass() != o.getClass())
                                return false;
                        ProductionKey that = (ProductionKey) o;
                        return idoportunidadobjetivo == that.idoportunidadobjetivo &&
                                        Objects.equals(anio, that.anio);
                }

                @Override
                public int hashCode() {
                        return Objects.hash(idoportunidadobjetivo, anio);
                }
        }
}