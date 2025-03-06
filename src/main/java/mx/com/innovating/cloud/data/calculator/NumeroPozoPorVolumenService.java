package mx.com.innovating.cloud.data.calculator;

import io.quarkus.cache.CacheKey;
import io.quarkus.cache.CacheResult;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.persistence.EntityManager;
import jakarta.persistence.NoResultException;
import jakarta.transaction.Transactional;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import mx.com.innovating.cloud.data.models.VolumetriaOportunidad;

import java.time.Year;
import java.util.List;

@ApplicationScoped
public class NumeroPozoPorVolumenService {

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static class NumeroPozoPorVolumenResult {
        private Integer vidoportunidadobjetivo;
        private Integer vpozoporvolumen;
        private Double veur;
    }

    @Inject
    EntityManager em;

    @Inject
    HiperbolicoAnualService hiperbolicoAnualService;

    @Transactional
    @CacheResult(cacheName = "volumetriaoportunidadtbl-cache")
    protected VolumetriaOportunidad getVolumetriaOportunidad(@CacheKey Integer pnoportunidadobjetivo,
            @CacheKey Integer percentil) {
        String query = """
                SELECT pce
                FROM catalogo.volumetriaoportunidadtbl
                WHERE idoportunidadobjetivo = :pnoportunidadobjetivo
                AND percentil = :percentil
                """;

        try {
            double pce = ((Number) em.createNativeQuery(query)
                    .setParameter("pnoportunidadobjetivo", pnoportunidadobjetivo)
                    .setParameter("percentil", percentil)
                    .getSingleResult()).doubleValue();

            return new VolumetriaOportunidad(pce);
        } catch (NoResultException e) {
            return null;
        }
    }

    public NumeroPozoPorVolumenResult spcNumeroPozoPorVolumen(
            Integer pnidversion,
            Integer pnoportunidadobjetivo,
            Integer pntipovalor,
            Integer pnmeses,
            Integer percentil) {

        List<HiperbolicoAnualService.HiperbolicoAnualResult> hiperbolicoResults = hiperbolicoAnualService
                .sppHiperbolicoAnual(pnidversion, pnoportunidadobjetivo, pntipovalor, pnmeses);


        Double w_eur = hiperbolicoResults.stream()
            .mapToDouble(result -> {
                int extractedYear = Integer.parseInt(result.getPanio());
                int daysInYear = Year.of(extractedYear).isLeap() ? 366 : 365; 
                return result.getPpmbpce() * daysInYear / 1000.0; 
            })
            .sum();

        // Get volumetria data and calculate w_pozoporvolumen exactly as in SPP:
        // pce/w_eur
        VolumetriaOportunidad volumetria = getVolumetriaOportunidad(pnoportunidadobjetivo, percentil);
        if (volumetria == null) {
            throw new RuntimeException("No volumetria data found for the given parameters");
        }

        Integer w_pozoporvolumen = (int) Math.round(volumetria.getPce() / w_eur);

        // Return exact fields as in SPP
        return new NumeroPozoPorVolumenResult(
                pnoportunidadobjetivo,
                w_pozoporvolumen,
                w_eur);
    }

    public NumeroPozoPorVolumenResult spcNumeroPozoPorVolumen(
            Integer pnidversion,
            Integer pnoportunidadobjetivo,
            Integer pntipovalor,
            Integer pnmeses) {

        Integer percentil = 66;
        return spcNumeroPozoPorVolumen(
                pnidversion,
                pnoportunidadobjetivo,
                pntipovalor,
                pnmeses,
                percentil);
    }
}