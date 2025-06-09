package com.pemex.pep.seemop.data.calculator;

import io.quarkus.cache.CacheKey;
import io.quarkus.cache.CacheResult;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.persistence.EntityManager;
import jakarta.transaction.Transactional;
import java.util.List;

@ApplicationScoped
public class FactoresCalculatorService {

    @Inject
    EntityManager em;

    @Transactional
    @CacheResult(cacheName = "factores-calculation")
    public List<Object[]> calcularFactores(@CacheKey int pnoportunidadobjetivo) {
        String query = """
                SELECT
                    CAST(mediaaceite AS double precision) / NULLIF(mediapce, 0) as faceite,
                    CAST(mediagas AS double precision) / NULLIF(mediapce, 0) as fgas,
                    CAST(mediacondensado AS double precision) / NULLIF(mediapce, 0) as fcondensado
                FROM catalogo.mediavolumetriaoportunidadtbl
                WHERE idoportunidadobjetivo = :pnoportunidadobjetivo
                """;

        return em.createNativeQuery(query)
                .setParameter("pnoportunidadobjetivo", pnoportunidadobjetivo)
                .getResultList();
    }
}