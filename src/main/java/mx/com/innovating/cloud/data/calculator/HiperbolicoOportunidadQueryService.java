package mx.com.innovating.cloud.data.calculator;

import io.quarkus.cache.CacheResult;
import io.quarkus.cache.CacheKey;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.persistence.EntityManager;
import jakarta.persistence.Query;
import java.util.List;
import java.util.stream.Collectors;
import mx.com.innovating.cloud.data.models.DatosHiperbolicosInput;
import lombok.extern.slf4j.Slf4j;
import jakarta.transaction.Transactional;

@ApplicationScoped
public class HiperbolicoOportunidadQueryService {
    @Inject
    EntityManager em;

    @CacheResult(cacheName = "datos-hiperbola-cache")
    protected List<DatosHiperbolicosInput> getDatosHiperbolicos(
            @CacheKey Integer pnidversion,
            @CacheKey Integer pnoportunidadobjetivo,
            @CacheKey Integer pntipovalor,
            @CacheKey Integer pnmeses) {

        String sql = """
                    SELECT
                        ga.idoportunidadobjetivo,
                        de.idoportunidad,
                        de.oportunidad,
                        de.claveoportunidad,
                        de.claveobjetivo,
                        CONCAT('01/01/', de.fechainicio) as fecha,
                        :pnmeses as meses,
                        (ga.gasto/(1+de.b*de.di*(0))^(1/de.b)) as hiperbolico,
                        ga.gasto,
                        de.b,
                        de.di,
                        de.rga,
                        de.rsimedio,
                        de.frcmedio,
                        de.declinacionmensual as dimen,
                        de.fechainicio as anio,
                        0 as dias,
                        o.idhidrocarburo,
                        o.hidrocarburo,
                        de.idversion
                    FROM catalogo.datoshiperbolicavw de
                    INNER JOIN catalogo.oportunidadvw o ON de.idoportunidad = o.idoportunidad
                    INNER JOIN catalogo.gastoinicialoportunidadvw ga
                        ON de.idoportunidadobjetivo = ga.idoportunidadobjetivo
                        AND de.idtipovalor = ga.idtipovalor
                    INNER JOIN catalogo.volumetriaoportunidadper50vw vol
                        ON vol.idoportunidadobjetivo = de.idoportunidadobjetivo
                    WHERE de.idversion = :idversion
                        AND de.idoportunidadobjetivo = :idoportunidadobjetivo
                        AND de.idtipovalor = :idtipovalor
                    GROUP BY
                        de.declinacionmensual,
                        ga.idoportunidadobjetivo,
                        ga.claveobjetivo,
                        de.idversion,
                        de.idoportunidad,
                        de.oportunidad,
                        de.claveobjetivo,
                        de.claveoportunidad,
                        de.di,
                        de.rga,
                        de.rsimedio,
                        de.frcmedio,
                        o.idhidrocarburo,
                        o.hidrocarburo,
                        de.b,
                        de.fechainicio,
                        ga.gasto,
                        ga.idtipovalor
                """;

        Query query = em.createNativeQuery(sql)
                .setParameter("idversion", pnidversion)
                .setParameter("idoportunidadobjetivo", pnoportunidadobjetivo)
                .setParameter("idtipovalor", pntipovalor)
                .setParameter("pnmeses", pnmeses);

        List<Object[]> results = query.getResultList();
        return mapToDatosHiperbolicos(results);
    }

    private List<DatosHiperbolicosInput> mapToDatosHiperbolicos(List<Object[]> results) {
        return results.stream()
                .map(this::mapRowToDatosHiperbolicos)
                .collect(Collectors.toList());
    }

    private DatosHiperbolicosInput mapRowToDatosHiperbolicos(Object[] row) {
        return new DatosHiperbolicosInput(
                (Integer) row[0], // idoportunidadobjetivo
                (Integer) row[1], // idoportunidad
                (String) row[2], // oportunidad
                (String) row[3], // claveoportunidad
                (String) row[4], // claveobjetivo
                (String) row[5], // fecha
                (Integer) row[6], // meses
                (Double) row[7], // hiperbolico
                (Double) row[8], // gastoInicial
                (Double) row[9], // b
                (Double) row[10], // di
                (Double) row[11], // rga
                (Double) row[12], // rsiMedio
                (Double) row[13], // frcMedio
                (Double) row[14], // dimen
                (String) row[15], // anio
                (Integer) row[16], // dias
                (Integer) row[17], // idTipoHidrocarburo
                (String) row[18], // hidrocarburo
                (Integer) row[19] // idVersion
        );
    }
}