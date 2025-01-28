package mx.com.innovating.cloud.data.calculator;

import io.quarkus.logging.Log;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.persistence.EntityManager;
import jakarta.persistence.NoResultException;
import jakarta.transaction.Transactional;
import io.quarkus.cache.CacheResult;
import io.quarkus.cache.CacheKey;
import mx.com.innovating.cloud.data.models.NumeroPozoAreaConvencionalResult;

@ApplicationScoped
public class NumeroPozoAreaConvencionalService {

    @Inject
    EntityManager em;

    private static final double PI = 3.14159265358979;

    @Transactional
    @CacheResult(cacheName = "radiodrene-cache")
    protected Double getRadioDrene(@CacheKey Integer pnidversion, @CacheKey Integer pnoportunidadobjetivo) {
        try {
            String query = """
                    SELECT r.radiodrene
                    FROM catalogo.reloportunidadobjetivotbl r
                    WHERE r.idversion = :idversion
                    AND r.idoportunidadobjetivo = :idoportunidadobjetivo
                    """;

            Object result = em.createNativeQuery(query)
                    .setParameter("idversion", pnidversion)
                    .setParameter("idoportunidadobjetivo", pnoportunidadobjetivo)
                    .getSingleResult();
            if (result instanceof Number) {
                return ((Number) result).doubleValue();
            } else if (result instanceof String) {
                return Double.parseDouble(result.toString());
            } else {
                throw new RuntimeException("Unexpected result type: " + result.getClass());
            }

        } catch (NoResultException e) {
            Log.error("No radio drene found for version " + pnidversion + " and oportunidad " + pnoportunidadobjetivo,
                    e);
            throw new RuntimeException("No radio drene found", e);
        } catch (Exception e) {
            Log.error("Error getting radio drene", e);
            throw new RuntimeException("Error getting radio drene", e);
        }
    }

    public NumeroPozoAreaConvencionalResult calcularNumeroPozoAreaConvencional(
            Integer pnidversion,
            Integer pnoportunidadobjetivo,
            Double pnarea) {

        try {
            // Get cached radio drene
            Double wRadioDrene = getRadioDrene(pnidversion, pnoportunidadobjetivo);

            // Calculate area drene
            Double wAreaDrene = PI * Math.pow(wRadioDrene, 2);

            // Calculate number of wells
            Double numPozo = pnarea / wAreaDrene;

            // Return result object
            return new NumeroPozoAreaConvencionalResult(
                    pnoportunidadobjetivo,
                    wAreaDrene,
                    numPozo);

        } catch (Exception e) {
            Log.error("Error in calcularNumeroPozoAreaConvencional", e);
            throw new RuntimeException("Error calculating numero pozo area convencional", e);
        }
    }
}