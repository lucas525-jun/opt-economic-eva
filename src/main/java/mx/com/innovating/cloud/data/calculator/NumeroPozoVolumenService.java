package mx.com.innovating.cloud.data.calculator;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.persistence.EntityManager;
import jakarta.persistence.Query;
import java.math.BigDecimal;
import java.util.List;
import java.util.ArrayList;
import mx.com.innovating.cloud.data.models.NumeroPozoVolumenResult;
import mx.com.innovating.cloud.data.models.HiperbolicoOportunidadResult;
import io.quarkus.cache.CacheResult;
import io.quarkus.cache.CacheKey;

@ApplicationScoped
public class NumeroPozoVolumenQueryService {
    @Inject
    EntityManager entityManager;

    @Inject
    HiperbolicoOportunidadService hiperbolicoOportunidadService;

    public Double calculateEurFromHiperbolicoAnual(
            Integer idVersion,
            Integer opportunityId,
            Integer tipoValor,
            Integer meses) {

        // Use the new service to get calculation results
        List<HiperbolicoOportunidadResult> results = hiperbolicoOportunidadService.calculateHiperbolicoOportunidad(
                idVersion,
                opportunityId,
                tipoValor,
                meses);

        if (results == null || results.isEmpty()) {
            return null;
        }

        double eurSum = results.stream()
                .mapToDouble(HiperbolicoOportunidadResult::getPpmbpce)
                .sum() * 365 / 1000;

        return eurSum;
    }

    @CacheResult(cacheName = "numero-pozo-volumen-pce-cache")

    protected Double getPceForOpportunity(@CacheKey Integer opportunityId) {
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