package mx.com.innovating.cloud.data.calculator;

import io.quarkus.logging.Log;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.persistence.EntityManager;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;

import io.quarkus.cache.CacheKey;
import io.quarkus.cache.CacheResult;
import jakarta.transaction.Transactional;
import mx.com.innovating.cloud.data.models.FechaInicioResult;

@ApplicationScoped
public class FechaInicioMultiObjetivoService {

    @Inject
    EntityManager em;

    @Inject
    PozoVolumenService pozoVolumenService;

    @Inject
    NumeroPozoAreaConvencionalService numeroPozoAreaService;

    @Inject
    NumEquipoService numEquipoService;

    @Transactional
    @CacheResult(cacheName = "tipo-calculo-cache")
    public String getTipoCalculo(
            @CacheKey Integer pnidversion,
            @CacheKey Integer pnoportunidadobjetivo) {
        String query = """
                    SELECT DISTINCT tipocalculo
                    FROM catalogo.oportunidadvw
                    WHERE idversion = :idversion AND idoportunidad =
                    (SELECT idoportunidad FROM catalogo.claveobjetivovw
                     WHERE idoportunidadobjetivo = :idoportunidadobjetivo
                     AND idversion = :idversion)
                """;

        return (String) em.createNativeQuery(query)
                .setParameter("idversion", pnidversion)
                .setParameter("idoportunidadobjetivo", pnoportunidadobjetivo)
                .getSingleResult();
    }

    @Transactional
    @CacheResult(cacheName = "clave-objetivo-details-cache")
    public ClaveObjetivoDetails getClaveObjetivoDetails(
            @CacheKey Integer pnoportunidadobjetivo,
            @CacheKey Integer pnidversion) {
        String query = """
                    SELECT fechainicio,
                           duracionperfpozodesarrollo,
                           duraciontermpozodesarrollo
                    FROM catalogo.claveobjetivovw
                    WHERE idoportunidadobjetivo = :idoportunidadobjetivo
                    AND idversion = :idversion
                    LIMIT 1
                """;

        Object[] result = (Object[]) em.createNativeQuery(query)
                .setParameter("idoportunidadobjetivo", pnoportunidadobjetivo)
                .setParameter("idversion", pnidversion)
                .getSingleResult();

        return new ClaveObjetivoDetails(
                result[0].toString(),
                ((Number) result[1]).doubleValue(),
                ((Number) result[2]).doubleValue());
    }

    public List<FechaInicioResult> sppFechaInicioMulti(
            Integer pnidversion,
            Integer pnoportunidadobjetivo,
            Double pncuota,
            Double pndeclinada,
            Double pnpce,
            Double pnarea) {

        return sppFechaInicioMulti(pnidversion,
                pnoportunidadobjetivo,
                pncuota,
                pndeclinada,
                pnpce,
                pnarea,
                null);
    }

    public List<FechaInicioResult> sppFechaInicioMulti(
            Integer pnidversion,
            Integer pnoportunidadobjetivo,
            Double pncuota,
            Double pndeclinada,
            Double pnpce,
            Double pnarea,
            LocalDateTime fecha) {

        try {
            String vartipocalculo = getTipoCalculo(pnidversion, pnoportunidadobjetivo);

            Integer varnpozos;
            switch (vartipocalculo) {
                case "Volumen":
                    varnpozos = (int) Math.ceil(pozoVolumenService.calcularPozoVolumen(
                            pnidversion, pnoportunidadobjetivo, pncuota, pndeclinada, pnpce)
                            .get(0).getCvnumpozo());
                    break;
                case "Area":
                    varnpozos = (int) Math.ceil(numeroPozoAreaService.calcularNumeroPozoAreaConvencional(
                            pnidversion, pnoportunidadobjetivo, pnarea).getNumPozo());
                    break;
                case "Ambos":
                    Integer pozoVolumen = (int) Math.ceil(pozoVolumenService.calcularPozoVolumen(
                            pnidversion, pnoportunidadobjetivo, pncuota, pndeclinada, pnpce)
                            .get(0).getCvnumpozo());

                    Integer pozoArea = (int) Math.ceil(numeroPozoAreaService.calcularNumeroPozoAreaConvencional(
                            pnidversion, pnoportunidadobjetivo, pnarea).getNumPozo());

                    varnpozos = Math.min(pozoVolumen, pozoArea);
                    break;
                default:
                    throw new IllegalArgumentException("Invalid calculation type: " + vartipocalculo);
            }

            Integer varnequipo = numEquipoService.calcularNumEquipo(varnpozos).getVEquipo();
            Integer varentrada = (int) Math.ceil((double) varnpozos / varnequipo);

            if (varnpozos <= 1) {
                varnequipo = 1;
                varentrada = 1;
            }

            ClaveObjetivoDetails claveObjetivoDetails = getClaveObjetivoDetails(
                    pnoportunidadobjetivo, pnidversion);

            String varanio = claveObjetivoDetails.getFechaInicio();
            Double varndiasperf = claveObjetivoDetails.getDuracionPerfPozoDesarrollo();
            Double varndiasterm = claveObjetivoDetails.getDuracionTermPozoDesarrollo();
            Double varndias = varndiasperf + varndiasterm;

            LocalDateTime varfechainicio;
            LocalDateTime varfechatermino;

            if (fecha == null) {
                varfechainicio = LocalDateTime.parse(varanio + "-01-01T00:00:00");
                varfechatermino = varfechainicio.plusDays(varndias.longValue());
            } else {
                varfechainicio = fecha;
                varfechatermino = fecha.withDayOfMonth(1).plusMonths(1);
            }

            List<FechaInicioResult> finalResults = new ArrayList<>();
            LocalDateTime currentFechainicio = varfechainicio;
            LocalDateTime currentFechatermino = varfechatermino;

            for (int currentIdCte = 1; currentIdCte <= varentrada; currentIdCte++) {
                Integer currentNPozo;
                if (currentIdCte == varentrada) {
                    currentNPozo = varnpozos;
                } else {
                    currentNPozo = varnequipo * currentIdCte;
                }

                LocalDateTime fechaEntrada = calculateFechaEntrada(currentFechatermino);

                finalResults.add(new FechaInicioResult(
                        fechaEntrada,
                        (double) getFinalMonth(currentFechatermino),
                        (double) getFinalYear(currentFechatermino),
                        currentFechainicio,
                        currentFechatermino,
                        varnequipo,
                        currentNPozo,
                        varentrada,
                        varndiasperf,
                        varndiasterm,
                        varndias,
                        currentIdCte));

                currentFechainicio = currentFechatermino;
                // Recalculate fechaTermino for the next iteration
                currentFechatermino = currentFechainicio.plusDays(varndias.longValue()); // Cast to long
                double fractionalDays = varndias - varndias.longValue();
                long fractionalSeconds = (long) (fractionalDays * 24 * 60 * 60);
                currentFechatermino = currentFechatermino.plusSeconds(fractionalSeconds);
            }

            return finalResults;

        } catch (Exception e) {
            Log.error("Error in sppFechaInicio2", e);
            throw new RuntimeException("Error in fecha inicio calculation", e);
        }
    }

    private LocalDateTime calculateFechaEntrada(LocalDateTime date) {
        LocalDateTime monthStart = date.withDayOfMonth(1);
        LocalDateTime nextMonthStart = monthStart.plusMonths(1);
        long daysInMonth = monthStart.plusMonths(1).minusDays(1).getDayOfMonth();
        long dayOfMonth = date.getDayOfMonth();

        double dayFraction = (double) (dayOfMonth - 1) / daysInMonth;

        return dayFraction > 0.5 ? nextMonthStart : date;
    }

    private int getFinalMonth(LocalDateTime date) {
        double dayFraction = (double) (date.getDayOfMonth() - 1) /
                date.getMonth().length(date.toLocalDate().isLeapYear());

        return dayFraction > 0.5 ? (date.getMonthValue() % 12) + 1 : date.getMonthValue();
    }

    private int getFinalYear(LocalDateTime date) {
        double dayFraction = (double) (date.getDayOfMonth() - 1) /
                date.getMonth().length(date.toLocalDate().isLeapYear());

        return dayFraction > 0.5 ? (date.getMonthValue() == 12 ? date.getYear() + 1 : date.getYear()) : date.getYear();
    }

    @Data
    @AllArgsConstructor
    public static class ClaveObjetivoDetails {
        private String fechaInicio;
        private Double duracionPerfPozoDesarrollo;
        private Double duracionTermPozoDesarrollo;
    }

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    private static class CalculoFechaTemp {
        Integer equipo;
        Integer pozo;
        Integer entrada;
        String anio;
        Double diasperf;
        Double diasterm;
        Double dias;
        LocalDateTime vfechainicio;
        LocalDateTime vfechatermino;
    }

}