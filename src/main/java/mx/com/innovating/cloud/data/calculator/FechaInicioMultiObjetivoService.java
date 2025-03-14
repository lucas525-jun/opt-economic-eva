package mx.com.innovating.cloud.data.calculator;

import io.quarkus.logging.Log;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.persistence.EntityManager;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;

import mx.com.innovating.cloud.data.models.FechaInicioResult;
import mx.com.innovating.cloud.data.models.CalculoNumPozosResult;

@ApplicationScoped
public class FechaInicioMultiObjetivoService {

    @Inject
    EntityManager em;

    @Inject
    PozoVolumenService pozoVolumenService;

    @Inject
    FechaInicioService fechaInicioService;

    @Inject
    NumeroPozoAreaConvencionalService numeroPozoAreaService;

    @Inject
    NumEquipoService numEquipoService;

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
            String vartipocalculo = fechaInicioService.obtenerTipoCalculo(pnidversion, pnoportunidadobjetivo);
            CalculoNumPozosResult calculoNumPozosResult = fechaInicioService.calcularNumeroPozos(vartipocalculo, pnidversion, pnoportunidadobjetivo,
            pncuota, pndeclinada, pnpce, pnarea);
            Integer varNPozos = (int) Math.ceil(calculoNumPozosResult.getNPozos());
            
            Integer varnequipo = numEquipoService.calcularNumEquipo(varNPozos).getVEquipo();
            Integer varentrada = (int) Math.ceil((double) varNPozos / varnequipo);

            if (varNPozos <= 1) {
                varnequipo = 1;
                varentrada = 1;
            }

            FechaInicioService.QueryDateInfo claveObjetivoDetails = fechaInicioService.obtenerInfoFechas(pnidversion, pnoportunidadobjetivo);

            String varanio = claveObjetivoDetails.anio;
            Double varndiasperf = claveObjetivoDetails.diasPerf;
            Double varndiasterm = claveObjetivoDetails.diasTerm;
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
                    currentNPozo = varNPozos;
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

        // return dayFraction > 0.5 ? nextMonthStart : date;
        return dayFraction > 0.5 ? date.plusMonths(1) : date;

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