package mx.com.innovating.cloud.data.calculator;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.persistence.EntityManager;
import jakarta.persistence.Query;
import jakarta.transaction.Transactional;

import java.math.BigDecimal;
import java.util.List;
import io.quarkus.cache.CacheResult;
import io.quarkus.cache.CacheKey;
import io.quarkus.cache.CacheResult;

@ApplicationScoped
public class NumeroPozoVolumenQueryService {
    @Inject
    EntityManager entityManager;

    /**
     * Retrieves the Annual Hyperbolic Calculation and calculates EUR
     * Strictly follows the original SPP logic
     */
    @Transactional
    @CacheResult(cacheName = "numero-pozo-volumen-eur-cache")

    public Double calculateEurFromHiperbolicoAnual(
            @CacheKey Integer idVersion,
            @CacheKey Integer opportunityId,
            @CacheKey Integer tipoValor,
            @CacheKey Integer meses) {
        // Directly match the original calculation: sum(ppmbpce) * 365 / 1000
        String sql = "SELECT (sum(ppmbpce)*365/1000) as eur " +
                "FROM calculo.spp_hiperbolicoanual(:idVersion, :opportunityId, :tipoValor, :meses)";

        Query query = entityManager.createNativeQuery(sql);
        query.setParameter("idVersion", idVersion);
        query.setParameter("opportunityId", opportunityId);
        query.setParameter("tipoValor", tipoValor);
        query.setParameter("meses", meses);

        List<?> results = query.getResultList();

        // Strict handling to match original function's behavior
        return results.isEmpty() ? null : ((Number) results.get(0)).doubleValue();
    }

    /**
     * Retrieves PCE for specific opportunity at percentile 66
     * Strictly follows the original volumetria query
     */
    @Transactional
    @CacheResult(cacheName = "numero-pozo-volumen-pce-cache")

    public Double getPceForOpportunity(@CacheKey Integer opportunityId) {
        String sql = "SELECT pce " +
                "FROM catalogo.volumetriaoportunidadtbl " +
                "WHERE idoportunidadobjetivo = :opportunityId " +
                "AND percentil = 66";

        Query query = entityManager.createNativeQuery(sql);
        query.setParameter("opportunityId", opportunityId);

        List<?> results = query.getResultList();

        // Strict handling to match original function's behavior
        return results.isEmpty() ? null : ((Number) results.get(0)).doubleValue();
    }
}