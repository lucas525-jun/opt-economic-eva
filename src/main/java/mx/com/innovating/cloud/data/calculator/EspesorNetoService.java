package mx.com.innovating.cloud.data.calculator;

import io.quarkus.cache.CacheKey;
import io.quarkus.cache.CacheResult;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.persistence.EntityManager;
import jakarta.transaction.Transactional;
import lombok.AllArgsConstructor;
import lombok.Data;
import java.util.ArrayList;
import java.util.List;

import mx.com.innovating.cloud.data.models.DetalleOportunidadNoconvencional;

@ApplicationScoped
public class EspesorNetoService {

    // Internal data class for return results
    @Data
    @AllArgsConstructor
    public static class EspesorNetoResult {
        private Double espesorneto;
        private Integer vidoportunidadobjetivo;
        private Integer vintervaloposible;
    }

    @Inject
    EntityManager em;

    @Transactional
    @CacheResult(cacheName = "detalle-oportunidad-data")
    protected DetalleOportunidadNoconvencional getDetalleOportunidad(@CacheKey Integer pnoportunidadobjetivo) {
        String query = """
                SELECT d.espesornetop10, d.espesornetop90, d.espesornetomedia
                FROM catalogo.detalleoportunidadnoconvencionaltbl d
                WHERE d.idoportunidadobjetivo = :pnoportunidadobjetivo
                """;

        Object[] result = (Object[]) em.createNativeQuery(query)
                .setParameter("pnoportunidadobjetivo", pnoportunidadobjetivo)
                .getSingleResult();

        return new DetalleOportunidadNoconvencional(
                ((Number) result[0]).doubleValue(),
                ((Number) result[1]).doubleValue(),
                ((Number) result[2]).doubleValue());
    }

    public List<EspesorNetoResult> spcEspesorneto(Double paleatorio, Integer pnoportunidadobjetivo) {
        List<EspesorNetoResult> results = new ArrayList<>();

        // Get cached data
        DetalleOportunidadNoconvencional detalle = getDetalleOportunidad(pnoportunidadobjetivo);

        // Declare variables exactly as in SPP
        double w_p10 = detalle.getEspesornetop10();
        double w_p90 = detalle.getEspesornetop90();
        double w_media = detalle.getEspesornetomedia();
        double w_espesor = 0.0;
        int w_intervaloposible = 0;

        // Follow exact SPP logic
        if ((w_p10 - w_p90) != 0) {
            if (paleatorio < (w_media - w_p90) / (w_p10 - w_p90)) {
                w_espesor = w_p90 + Math.sqrt(paleatorio * (w_media - w_p90) * (w_p10 - w_p90));
            } else {
                w_espesor = w_p10 - Math.sqrt((1 - paleatorio) * (w_media - w_p90) * (w_p10 - w_p90));
            }
        } else {
            w_espesor = w_media;
        }

        if (w_espesor < 120) {
            w_intervaloposible = 1;
        } else {
            w_intervaloposible = (int) Math.round(w_espesor / 120.0);
        }

        results.add(new EspesorNetoResult(w_espesor, pnoportunidadobjetivo, w_intervaloposible));
        return results;
    }
}