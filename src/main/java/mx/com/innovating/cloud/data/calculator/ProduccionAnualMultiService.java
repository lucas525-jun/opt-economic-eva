package mx.com.innovating.cloud.data.calculator;

import io.quarkus.cache.CacheResult;
import io.quarkus.cache.CacheKey;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.persistence.EntityManager;
import jakarta.persistence.Query;
import jakarta.transaction.Transactional;

import mx.com.innovating.cloud.data.models.VectorProduccion;
import mx.com.innovating.cloud.data.models.EscaleraProduccionMulti;
import mx.com.innovating.cloud.data.calculator.EscaleraProduccionMultiService;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.Comparator;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

@ApplicationScoped
public class ProduccionAnualMultiService {

        private static final BigDecimal THOUSAND = BigDecimal.valueOf(1000);
        private static final BigDecimal DAYS_IN_YEAR = BigDecimal.valueOf(365);

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

                // Get escalera produccion data
                List<EscaleraProduccionMulti> escaleraResults = escaleraProduccionMultiService
                                .calculateEscaleraProduccionMulti(pnidversion, vidoportunidadobjetivo,
                                                pncuota, pndeclinada, pnpce, pnarea, vvfecha);

                // Group production results with BigDecimal for precise calculations
                Map<ProductionKey, ProductionAggregates> groupedProduction = escaleraResults.stream()
                                .collect(Collectors.groupingBy(
                                                er -> new ProductionKey(
                                                                er.getIdoportunidadobjetivo(),
                                                                er.getAnio()),
                                                Collectors.collectingAndThen(
                                                                Collectors.toList(),
                                                                list -> {
                                                                        BigDecimal sumProduccion = list.stream()
                                                                                        .map(er -> BigDecimal.valueOf(er
                                                                                                        .getProduccion()))
                                                                                        .reduce(BigDecimal.ZERO,
                                                                                                        BigDecimal::add);
                                                                        BigDecimal sumProdAceite = list.stream()
                                                                                        .map(er -> BigDecimal.valueOf(er
                                                                                                        .getProdaceite()))
                                                                                        .reduce(BigDecimal.ZERO,
                                                                                                        BigDecimal::add);
                                                                        BigDecimal sumProdGas = list.stream()
                                                                                        .map(er -> BigDecimal.valueOf(er
                                                                                                        .getProdgas()))
                                                                                        .reduce(BigDecimal.ZERO,
                                                                                                        BigDecimal::add);
                                                                        BigDecimal sumProdCondensado = list.stream()
                                                                                        .map(er -> BigDecimal.valueOf(er
                                                                                                        .getProdcondensado()))
                                                                                        .reduce(BigDecimal.ZERO,
                                                                                                        BigDecimal::add);
                                                                        return new ProductionAggregates(sumProduccion,
                                                                                        sumProdAceite, sumProdGas,
                                                                                        sumProdCondensado);
                                                                })));

                // Fetch catalog data
                List<Object[]> catalogData = getOportunidadObjetivoData(vidoportunidadobjetivo);

                // Transform to VectorProduccion with precise calculations
                return groupedProduction.entrySet().stream()
                                .map(entry -> {
                                        ProductionKey key = entry.getKey();
                                        ProductionAggregates aggregates = entry.getValue();

                                        // Precise calculations for all production types
                                        BigDecimal totalMes = aggregates.produccion.multiply(THOUSAND)
                                                        .setScale(10, RoundingMode.HALF_EVEN);
                                        BigDecimal totalAnual = totalMes.divide(DAYS_IN_YEAR, 10,
                                                        RoundingMode.HALF_EVEN);

                                        BigDecimal totalAceiteAnual = aggregates.prodAceite.multiply(THOUSAND)
                                                        .divide(DAYS_IN_YEAR, 10, RoundingMode.HALF_EVEN);
                                        BigDecimal totalGasAnual = aggregates.prodGas.multiply(THOUSAND)
                                                        .divide(DAYS_IN_YEAR, 10, RoundingMode.HALF_EVEN);
                                        BigDecimal totalCondensadoAnual = aggregates.prodCondensado.multiply(THOUSAND)
                                                        .divide(DAYS_IN_YEAR, 10, RoundingMode.HALF_EVEN);

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
                                                        totalAceiteAnual.doubleValue(), // ctaceiteanual
                                                        totalGasAnual.doubleValue(), // ctgasanual
                                                        totalCondensadoAnual.doubleValue())) // ctcondensadoanual
                                                        .orElse(null);
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

        private static class ProductionAggregates {
                final BigDecimal produccion;
                final BigDecimal prodAceite;
                final BigDecimal prodGas;
                final BigDecimal prodCondensado;

                ProductionAggregates(BigDecimal produccion, BigDecimal prodAceite, BigDecimal prodGas,
                                BigDecimal prodCondensado) {
                        this.produccion = produccion;
                        this.prodAceite = prodAceite;
                        this.prodGas = prodGas;
                        this.prodCondensado = prodCondensado;
                }
        }
}