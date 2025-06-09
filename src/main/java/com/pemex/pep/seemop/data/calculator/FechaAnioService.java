package com.pemex.pep.seemop.data.calculator;

import io.quarkus.cache.CacheKey;
import io.quarkus.cache.CacheResult;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.persistence.EntityManager;
import jakarta.transaction.Transactional;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import java.util.ArrayList;
import java.util.List;
import java.time.LocalDate;
import java.time.Year;


@ApplicationScoped
public class FechaAnioService {
        @Data
        @AllArgsConstructor
        @NoArgsConstructor
        public static class FechaAnioResult {
                private Integer vid;
                private Integer npozo;
                private String anio;
                private Integer vnpozo;
        }

        @Data
        @AllArgsConstructor
        @NoArgsConstructor
        private static class CalculoFechaH {
                private Integer idf;
                private Integer pozo;
                private String vanio;
                private Integer npozo;
        }

        @Data
        @AllArgsConstructor
        @NoArgsConstructor
        private static class CalculoPozoH {
                private Integer idp;
                private Integer pozo;
                private String vanio;
                private Integer npozo;
        }

        @Inject
        EntityManager em;

        @Inject
        NumeroPozoPorVolumenService numeroPozoPorVolumenService;

        @Inject
        NumeroPozoporAreaService numeroPozoporAreaService;

        @Transactional
        @CacheResult(cacheName = "rel-oportunidad-objetivo-cache")
        protected Integer getVarIdOportunidad(@CacheKey Integer pnidversion, @CacheKey Integer pnoportunidadobjetivo) {
                String query = """
                                SELECT idoportunidad
                                FROM catalogo.reloportunidadobjetivotbl
                                WHERE idversion = :pnidversion
                                AND idoportunidadobjetivo = :pnoportunidadobjetivo
                                """;
                return ((Number) em.createNativeQuery(query)
                                .setParameter("pnidversion", pnidversion)
                                .setParameter("pnoportunidadobjetivo", pnoportunidadobjetivo)
                                .getSingleResult()).intValue();
        }

        @Transactional
        @CacheResult(cacheName = "oportunidad-vw-cache")
        protected String getVarTipoCalculo(@CacheKey Integer varidoportunidad) {
                String query = """
                                SELECT DISTINCT tipocalculo
                                FROM catalogo.oportunidadvw
                                WHERE idoportunidad = :varidoportunidad
                                """;
                return (String) em.createNativeQuery(query)
                                .setParameter("varidoportunidad", varidoportunidad)
                                .getSingleResult();
        }

        @Transactional
        @CacheResult(cacheName = "firstcalculo-pozoh-cache")
        protected List<Object[]> getFirstCalculoPozoHData(@CacheKey Integer pnidversion,
                        @CacheKey Integer pnoportunidadobjetivo) {
                String query = """
                                SELECT pozoprograma, fechapruebapiloto, pozoprograma
                                FROM catalogo.detalleoportunidadnoconvencionaltbl
                                WHERE idoportunidadobjetivo = :pnoportunidadobjetivo
                                AND idversion = :pnidversion
                                """;
                return em.createNativeQuery(query)
                                .setParameter("pnidversion", pnidversion)
                                .setParameter("pnoportunidadobjetivo", pnoportunidadobjetivo)
                                .getResultList();
        }

        @Transactional
        protected List<Object[]> getSecondCalculoPozoHData(Integer pnidversion, Integer pnoportunidadobjetivo) {
                String query = """
                                SELECT det.equipoperforacion, det.fechadesarrollo,
                                       pa.totaltraslado, det.lapsoterminacion
                                FROM catalogo.detalleoportunidadnoconvencionaltbl det
                                INNER JOIN catalogo.parametrotrasladotbl pa
                                    ON det.idoportunidad = pa.idoportunidad
                                WHERE det.idoportunidadobjetivo = :pnoportunidadobjetivo
                                AND det.idversion = :pnidversion
                                """;
                return em.createNativeQuery(query)
                                .setParameter("pnidversion", pnidversion)
                                .setParameter("pnoportunidadobjetivo", pnoportunidadobjetivo)
                                .getResultList();
        }

        @Transactional
        public List<FechaAnioResult> spcFechaAnio(
                        Integer pnidversion,
                        Integer pnoportunidadobjetivo,
                        Integer pntipovalor,
                        Integer pnmeses,
                        Double paleatorio) {
                Integer percentil = 66;
                return spcFechaAnio(
                                pnidversion,
                                pnoportunidadobjetivo,
                                pntipovalor,
                                pnmeses,
                                paleatorio,
                                percentil

                );
        }

        @Transactional
        public List<FechaAnioResult> spcFechaAnio(
                        Integer pnidversion,
                        Integer pnoportunidadobjetivo,
                        Integer pntipovalor,
                        Integer pnmeses,
                        Double paleatorio,
                        Integer percentil) {

                final Integer varidoportunidad = getVarIdOportunidad(pnidversion, pnoportunidadobjetivo);
                final String vartipocalculo = getVarTipoCalculo(varidoportunidad);
                final Integer varnpozovol = numeroPozoPorVolumenService.spcNumeroPozoPorVolumen(
                                pnidversion, pnoportunidadobjetivo, pntipovalor, pnmeses, percentil)
                                .getVpozoporvolumen();
                final Integer varnpozoarea = numeroPozoporAreaService.spcNumeropozoporarea(
                                pnidversion, pnoportunidadobjetivo, paleatorio, 66).get(0).getW_pozoporarea();

                List<CalculoFechaH> calculoFechaH = new ArrayList<>();
                List<CalculoPozoH> calculoPozoH = new ArrayList<>();
                List<FechaAnioResult> results = new ArrayList<>();
                try {

                        // Process first data set
                        List<Object[]> firstData = getFirstCalculoPozoHData(pnidversion, pnoportunidadobjetivo);
                        for (Object[] row : firstData) {
                                calculoPozoH.add(new CalculoPozoH(
                                                1,
                                                ((Number) row[0]).intValue(),
                                                row[1].toString(),
                                                ((Number) row[2]).intValue()));
                        }

                        // Process second data set
                        List<Object[]> secondData = getSecondCalculoPozoHData(pnidversion, pnoportunidadobjetivo);
                        for (Object[] row : secondData) {
                                int equipoPerforacion = ((Number) row[0]).intValue();
                                String fechaDesarrollo = (String) row[1];
                                int extractedYear = LocalDate.parse(fechaDesarrollo).getYear(); 
                                
                                double totalTraslado = ((Number) row[2]).doubleValue();
                                double lapsoTerminacion = ((Number) row[3]).doubleValue();
                                int daysInYear = Year.of(extractedYear).isLeap() ? 366 : 365;

                                int pozos = (int) Math.floor(daysInYear / (totalTraslado + lapsoTerminacion))
                                                * equipoPerforacion;

                                int npozo;
                                if ("Volumen".equals(vartipocalculo)) {
                                        npozo = varnpozovol;
                                } else if ("Area".equals(vartipocalculo)) {
                                        npozo = varnpozoarea;
                                } else {
                                        npozo = Math.min(varnpozovol, varnpozoarea);
                                }

                                calculoPozoH.add(new CalculoPozoH(
                                                2,
                                                pozos,
                                                fechaDesarrollo,
                                                npozo));
                        }

                        // Get initial values
                        CalculoPozoH firstRecord = calculoPozoH.get(0);
                        int baseYear = Integer.parseInt(firstRecord.getVanio());
                        int basePozo = firstRecord.getPozo();
                        int increment = calculoPozoH.get(1).getPozo();
                        int totalPozos = calculoPozoH.get(1).getNpozo();

                        // Add first record
                        results.add(new FechaAnioResult(1, basePozo, String.valueOf(baseYear), basePozo));

                        // Generate progressive records
                        int currentPozo = basePozo;
                        for (int i = 2; i <= 9; i++) {
                                currentPozo += 2; // Increment by 2 each time
                                results.add(new FechaAnioResult(
                                                i,
                                                currentPozo,
                                                String.valueOf(baseYear + i - 1),
                                                increment));
                        }

                        // Add final record
                        results.add(new FechaAnioResult(
                                        10,
                                        totalPozos,
                                        String.valueOf(baseYear + 9),
                                        totalPozos));

                        return results;
                } finally {
                        // Clean up the lists
                        if (calculoFechaH != null) {
                                calculoFechaH.clear();
                                calculoFechaH = null;
                        }
                        if (calculoPozoH != null) {
                                calculoPozoH.clear();
                                calculoPozoH = null;
                        }

                }
        }
}