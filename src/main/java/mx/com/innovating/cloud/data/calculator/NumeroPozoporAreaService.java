package mx.com.innovating.cloud.data.calculator;

import io.quarkus.cache.CacheKey;
import io.quarkus.cache.CacheResult;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.persistence.EntityManager;
import jakarta.transaction.Transactional;
import lombok.AllArgsConstructor;
import lombok.Data;
import mx.com.innovating.cloud.data.models.VolumetriaOportunidad;
import mx.com.innovating.cloud.data.models.DetalleOportunidadNoConvencionalArea;
import java.util.ArrayList;
import java.util.List;

@ApplicationScoped
public class NumeroPozoporAreaService {

        // Service level variables to match SPP declarations
        private String w_sql = "";
        private String w_sqlmodulo = "";

        @Data
        @AllArgsConstructor
        public static class NumeroPozoporAreaResult {
                private Integer pnoportunidadobjetivo;
                private Integer w_pozoporarea;
                private Integer w_intervalo;
        }

        @Inject
        EntityManager em;

        @Inject
        EspesorNetoService espesorNetoService;

        // Constructor to initialize variables like SPP
        public NumeroPozoporAreaService() {
                this.w_sql = "";
                this.w_sqlmodulo = "";
        }

        @Transactional
        @CacheResult(cacheName = "volumetria-oportunidad-data")
        protected VolumetriaOportunidad getVolumetriaOportunidad(@CacheKey Integer pnoportunidadobjetivo,
                        @CacheKey double percentil) {
                String query = """
                                SELECT area
                                FROM catalogo.volumetriaoportunidadtbl
                                WHERE idoportunidadobjetivo = :pnoportunidadobjetivo
                                AND percentil = :percentil
                                """;

                double area = ((Number) em.createNativeQuery(query)
                                .setParameter("pnoportunidadobjetivo", pnoportunidadobjetivo)
                                .setParameter("percentil", percentil)
                                .getSingleResult()).doubleValue();

                return new VolumetriaOportunidad(area);
        }

        @Transactional
        @CacheResult(cacheName = "detalle-oportunidad-noconvencional-area-data")
        protected DetalleOportunidadNoConvencionalArea getDetalleOportunidad(@CacheKey Integer pnoportunidadobjetivo) {
                String query = """
                                SELECT areaprospectiva, longitudlateral, espaciamientoentrepozos
                                FROM catalogo.detalleoportunidadnoconvencionaltbl
                                WHERE idoportunidadobjetivo = :pnoportunidadobjetivo
                                """;

                Object[] result = (Object[]) em.createNativeQuery(query)
                                .setParameter("pnoportunidadobjetivo", pnoportunidadobjetivo)
                                .getSingleResult();

                return new DetalleOportunidadNoConvencionalArea(
                                ((Number) result[0]).doubleValue(),
                                ((Number) result[1]).doubleValue(),
                                ((Number) result[2]).doubleValue());
        }

        public List<NumeroPozoporAreaResult> spcNumeropozoporarea(
                        Integer pnidversion,
                        Integer pnoportunidadobjetivo,
                        Double paleatorio) {
                Integer percentil = 66;
                return spcNumeropozoporarea(
                                pnidversion,
                                pnoportunidadobjetivo,
                                paleatorio,
                                percentil);
        }

        public List<NumeroPozoporAreaResult> spcNumeropozoporarea(
                        Integer pnidversion,
                        Integer pnoportunidadobjetivo,
                        Double paleatorio,
                        Integer percentil) {

                // Initialize empty strings as in SPP
                this.w_sql = "";
                this.w_sqlmodulo = "";

                List<NumeroPozoporAreaResult> results = new ArrayList<>();

                // Declare variables exactly as in SPP
                int w_pozoporarea;
                int w_intervalo;
                double w_area;
                double w_areaprospectiva;
                double w_logitud;
                double w_espaciamiento;

                // Get cached data
                VolumetriaOportunidad volumetria = getVolumetriaOportunidad(pnoportunidadobjetivo, percentil);
                DetalleOportunidadNoConvencionalArea detalle = getDetalleOportunidad(pnoportunidadobjetivo);

                // Assign values following SPP order
                w_area = volumetria.getPce();
                w_areaprospectiva = detalle.getAreaprospectiva() / 100;
                w_logitud = detalle.getLongitudlateral();
                w_espaciamiento = detalle.getEspaciamientoentrepozos();

                // Get w_intervalo using EspesorNetoService
                List<EspesorNetoService.EspesorNetoResult> espesorResults = espesorNetoService
                                .spcEspesorneto(paleatorio, pnoportunidadobjetivo);
                w_intervalo = espesorResults.get(0).getVintervaloposible();

                // Calculate w_pozoporarea following the original SPP logic
                w_pozoporarea = (int) Math.floor(
                                ((w_area * w_areaprospectiva / (w_logitud / 1000 * w_espaciamiento / 1000))
                                                * w_intervalo));

                // Return results following SPP order
                results.add(new NumeroPozoporAreaResult(
                                pnoportunidadobjetivo,
                                w_pozoporarea,
                                w_intervalo));

                return results;
        }
}