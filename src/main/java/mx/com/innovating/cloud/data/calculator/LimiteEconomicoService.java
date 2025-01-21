package mx.com.innovating.cloud.data.calculator;

import io.quarkus.cache.CacheKey;
import io.quarkus.cache.CacheResult;
import io.quarkus.logging.Log;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.persistence.EntityManager;
import jakarta.transaction.Transactional;
import mx.com.innovating.cloud.data.models.LimiteEconomicoResult;
import java.util.List;

@ApplicationScoped
public class LimiteEconomicoService {

    @Inject
    EntityManager em;

    @Transactional
    @CacheResult(cacheName = "oportunidad-base-data")
    protected List<Object[]> getBaseData(
            @CacheKey int pnidversion,
            @CacheKey int pnoportunidadobjetivo) {

        String query = """
                    SELECT o.idOportunidad, o.oportunidad
                    FROM catalogo.claveobjetivovw o
                    WHERE o.idVersion = :pnidversion
                    AND o.idOportunidadObjetivo = :pnoportunidadobjetivo
                """;

        return em.createNativeQuery(query)
                .setParameter("pnidversion", pnidversion)
                .setParameter("pnoportunidadobjetivo", pnoportunidadobjetivo)
                .getResultList();
    }

    public List<LimiteEconomicoResult> calcularLimiteEconomico(
            int pnidversion,
            int pnoportunidadobjetivo,
            double pnpce) {

        // Get cached base data
        List<Object[]> baseData = getBaseData(pnidversion, pnoportunidadobjetivo);

        if (baseData.isEmpty()) {
            return List.of();
        }
        final double anioLimite = calculateAnioLimite(pnpce);

        // Map base data to result with safe type conversion
        return baseData.stream()
                .map(row -> {
                    // Safely convert the database values
                    Integer idOportunidad = (row[0] instanceof Integer) ? (Integer) row[0]
                            : Integer.valueOf(row[0].toString());

                    String oportunidad = (row[1] != null) ? row[1].toString() : "";

                    return new LimiteEconomicoResult(
                            pnoportunidadobjetivo,
                            idOportunidad,
                            oportunidad,
                            pnpce,
                            (int) anioLimite,
                            (int) anioLimite * 12);
                })
                .toList();
    }

    private double calculateAnioLimite(double pnpce) {
        // Calculate economic limit based on pnpce
        double anioLimite = -0.00003 * Math.pow(pnpce, 2) + (pnpce * 0.068) + 4.3824;

        // Conditional for low anioLimite and high pnpce
        if (anioLimite < 1 && pnpce > 1000) {
            anioLimite = 45;
        }

        // Round the value
        return Math.round(anioLimite);
    }

}