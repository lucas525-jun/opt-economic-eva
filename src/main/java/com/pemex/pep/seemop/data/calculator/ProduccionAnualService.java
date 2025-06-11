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
import java.math.RoundingMode;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;
import java.time.Year;

@ApplicationScoped
public class ProduccionAnualService {

        private static final BigDecimal THOUSAND = BigDecimal.valueOf(1000);

        @Inject
        EntityManager em;

        @Inject
        EscaleraProduccionService escaleraProduccionService;

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

        public List<VectorProduccion> calculateProduccionAnual(
                        int pnidversion,
                        int vidoportunidadobjetivo,
                        double pncuota,
                        double pndeclinada,
                        double pnpce,
                        double pnarea) {
                // Get escalera produccion data
                List<EscaleraProduccionMulti> escaleraResults = escaleraProduccionService
                                .calculateEscaleraProduccion(pnidversion, vidoportunidadobjetivo,
                                                pncuota, pndeclinada, pnpce, pnarea);

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
                List<Object[]> catalogData = getOportunidadObjetivoData(vidoportunidadobjetivo);

                // Transform to VectorProduccion with precise calculations
                return groupedProduction.entrySet().stream()
                        .map(entry -> {
                                ProductionKey key = entry.getKey();
                                BigDecimal sumProduccion = entry.getValue();
                        
                                // Extract year dynamically
                                int extractedYear = Integer.parseInt(key.aanio);
                                BigDecimal daysInYear = BigDecimal.valueOf(Year.of(extractedYear).isLeap() ? 366 : 365);
                        
                                // Precise monthly and annual calculations
                                BigDecimal totalMes = sumProduccion.multiply(THOUSAND)
                                                .setScale(10, RoundingMode.HALF_EVEN);
                                BigDecimal totalAnual = totalMes.divide(daysInYear, 10, RoundingMode.HALF_EVEN); // Dynamic year calculation
                        
                                // Find matching catalog entry
                                Optional<Object[]> catalogInfo = catalogData.stream()
                                                .filter(row -> (Integer) row[2] == key.vidoportunidadobjetivo)
                                                .findFirst();
                        
                                return catalogInfo.map(info -> new VectorProduccion(
                                                (String) info[0], // voportunidad
                                                (String) info[1], // vobjetivo
                                                key.vidoportunidadobjetivo, // vidoportunidadobjetivo
                                                key.aanio, // aanio
                                                totalMes.doubleValue(), // ctotalmes
                                                totalAnual.doubleValue(), // ctotalanual
                                                0.0,
                                                0.0,
                                                0.0)).orElse(null);
                                })
                                .filter(Objects::nonNull)
                                .sorted(Comparator.comparing(VectorProduccion::getAanio))
                                .collect(Collectors.toList());
        }

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