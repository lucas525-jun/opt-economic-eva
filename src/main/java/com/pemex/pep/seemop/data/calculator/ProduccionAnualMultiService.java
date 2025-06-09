package com.pemex.pep.seemop.data.calculator;

import io.quarkus.cache.CacheResult;
import io.quarkus.cache.CacheKey;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.persistence.EntityManager;
import jakarta.persistence.Query;
import jakarta.transaction.Transactional;

import com.pemex.pep.seemop.data.models.VectorProduccion;
import com.pemex.pep.seemop.data.models.EscaleraProduccionMulti;

import java.math.BigDecimal;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

@ApplicationScoped
public class ProduccionAnualMultiService {

    private static final BigDecimal THOUSAND = BigDecimal.valueOf(1000);

    @Inject
    EntityManager em;

    @Inject
    EscaleraProduccionMultiService escaleraProduccionMultiService;

    @Transactional
    @CacheResult(cacheName = "oportunidad-objetivo-cache")
    public List<Object[]> getOportunidadObjetivoData(@CacheKey int vidoportunidadobjetivo) {
        String sql = """
                SELECT cv.oportunidad, cv.objetivo, cv.idoportunidadobjetivo
                FROM catalogo.datosoportunidadobjetivovw cv
                WHERE cv.idoportunidadobjetivo = :vidoportunidadobjetivo
                """;

        Query query = em.createNativeQuery(sql)
                .setParameter("vidoportunidadobjetivo", vidoportunidadobjetivo);

        return query.getResultList();
    }

    public List<VectorProduccion> calculateProduccionAnualMulti(
            int pnidversion,
            int vidoportunidadobjetivo,
            double pncuota,
            double pndeclinada,
            double pnpce,
            double pnarea,
            String vvfecha) {

        // Get escalera produccion data - this matches the SQL's INSERT INTO calculoanual

        List<EscaleraProduccionMulti> escaleraResults = escaleraProduccionMultiService
                .calculateEscaleraProduccionMulti(pnidversion, vidoportunidadobjetivo,
                        pncuota, pndeclinada, pnpce, pnarea, vvfecha);
        
        // Group production data by year - matches GROUP BY in SQL
        Map<ProductionKey, List<EscaleraProduccionMulti>> groupedProduction = escaleraResults.stream()
                .collect(Collectors.groupingBy(
                        er -> new ProductionKey(
                                er.getIdoportunidadobjetivo(),
                                er.getAnio())
                ));
        // Fetch catalog data - this matches joining with catalogo.datosoportunidadobjetivovw
        List<Object[]> catalogData = getOportunidadObjetivoData(vidoportunidadobjetivo);

        // Process each group to calculate annual values
        List<VectorProduccion> result = groupedProduction.entrySet().stream()
                .map(entry -> {
                    ProductionKey key = entry.getKey();
                    List<EscaleraProduccionMulti> yearData = entry.getValue();
                    
                    // Sum up production values using double directly to match SQL's SUM
                    double sumProduccion = yearData.stream()
                    .mapToDouble(EscaleraProduccionMulti::getProduccion)
                    .sum();
                    double sumProdAceite = yearData.stream()
                            .mapToDouble(EscaleraProduccionMulti::getProdaceite)
                            .sum();
                    double sumProdGas = yearData.stream()
                            .mapToDouble(EscaleraProduccionMulti::getProdgas)
                            .sum();
                    double sumProdCondensado = yearData.stream()
                            .mapToDouble(EscaleraProduccionMulti::getProdcondensado)
                            .sum();

                    // Parse year and determine days in year using the same logic as SQL
                    int year = Integer.parseInt(key.aanio);
                    double daysInYear = isLeapYear(year) ? 366.0 : 365.0;

                    // Calculate totals directly matching SQL formula syntax
                    double totalMes = sumProduccion * 1000;
                    double totalAnual = (sumProduccion * 1000) / daysInYear;
                    double totalAceiteAnual = (sumProdAceite * 1000) / daysInYear;
                    double totalGasAnual = (sumProdGas * 1000) / daysInYear;
                    double totalCondensadoAnual = (sumProdCondensado * 1000) / daysInYear;

                    // Find matching catalog entry
                    Optional<Object[]> catalogInfo = catalogData.stream()
                            .filter(row -> (Integer) row[2] == key.vidoportunidadobjetivo)
                            .findFirst();

                    // Create VectorProduccion with exact same field mapping as SQL
                    return catalogInfo.map(info -> new VectorProduccion(
                            (String) info[0],               // voportunidad
                            (String) info[1],               // vobjetivo
                            key.vidoportunidadobjetivo,     // vidoportunidadobjetivo
                            key.aanio,                      // aanio
                            totalMes,                       // ctotalmes
                            totalAnual,                     // ctotalanual
                            totalAceiteAnual,               // ctaceiteanual
                            totalGasAnual,                  // ctgasanual
                            totalCondensadoAnual))          // ctcondensadoanual
                            .orElse(null);
                })
                .filter(Objects::nonNull)
                .sorted(Comparator.comparing(VectorProduccion::getAanio))
                .collect(Collectors.toList());

        return result;
    }

    /**
     * Calculate if a year is a leap year using the same logic as the SQL function
     */
    private boolean isLeapYear(int year) {
        return (year % 4 == 0 && year % 100 != 0) || (year % 400 == 0);
    }

    /**
     * Key class for grouping production data by opportunity-objective ID and year
     */
    private static class ProductionKey {
        final int vidoportunidadobjetivo;
        final String aanio;

        ProductionKey(int vidoportunidadobjetivo, String aanio) {
            this.vidoportunidadobjetivo = vidoportunidadobjetivo;
            this.aanio = aanio;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o)
                return true;
            if (o == null || getClass() != o.getClass())
                return false;
            ProductionKey that = (ProductionKey) o;
            return vidoportunidadobjetivo == that.vidoportunidadobjetivo &&
                   Objects.equals(aanio, that.aanio);
        }

        @Override
        public int hashCode() {
            return Objects.hash(vidoportunidadobjetivo, aanio);
        }
    }
}