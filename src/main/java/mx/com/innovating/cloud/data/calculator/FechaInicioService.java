package mx.com.innovating.cloud.data.calculator;

import io.quarkus.cache.CacheResult;
import io.quarkus.cache.CacheKey;
import io.quarkus.logging.Log;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.persistence.EntityManager;
import jakarta.persistence.Query;
import jakarta.transaction.Transactional;

import mx.com.innovating.cloud.data.models.FechaInicioResult;
import mx.com.innovating.cloud.data.models.ProduccionPozos;
import mx.com.innovating.cloud.data.models.NumEquipoResult;
import mx.com.innovating.cloud.data.models.NumeroPozoAreaConvencionalResult;
import mx.com.innovating.cloud.data.models.ProduccionPozos;
import mx.com.innovating.cloud.data.calculator.PozoVolumenService;
import mx.com.innovating.cloud.data.calculator.NumEquipoService;
import java.math.BigDecimal;

import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.sql.Timestamp;
import java.math.BigDecimal;

@ApplicationScoped
public class FechaInicioService {

    @Inject
    EntityManager em;

    @Inject
    PozoVolumenService pozoVolumenService;

    @Inject
    NumEquipoService numEquipoService;

    @Inject
    NumeroPozoAreaConvencionalService numeroPozoAreaConvencionalService;

    public List<FechaInicioResult> calcularFechaInicio(
            Integer pnIdVersion,
            Integer pnOportunidadObjetivo,
            Double pnCuota,
            Double pnDeclinada,
            Double pnPce,
            Double pnArea) {

        // Get tipo calculo
        String tipoCalculo = obtenerTipoCalculo(pnIdVersion, pnOportunidadObjetivo);

        // Calculate number of wells based on tipo calculo
        Integer varNPozos = calcularNumeroPozos(tipoCalculo, pnIdVersion, pnOportunidadObjetivo,
                pnCuota, pnDeclinada, pnPce, pnArea);

        // Get equipment number
        NumEquipoResult numEquipo = numEquipoService.calcularNumEquipo(varNPozos);
        Integer varNEquipo = numEquipo.getVEquipo();

        // Calculate entrada (ceiling of pozos/equipo)
        Integer varEntrada = (int) Math.ceil((double) varNPozos / varNEquipo);

        // Get initial dates and durations
        QueryDateInfo dateInfo = obtenerInfoFechas(pnIdVersion, pnOportunidadObjetivo);

        // Calculate total days
        Double varNDias = dateInfo.diasPerf + dateInfo.diasTerm;

        // Get initial dates
        LocalDateTime varFechaInicio = obtenerFechaInicio(pnOportunidadObjetivo);
        LocalDateTime varFechaTermino = varFechaInicio.plusDays(varNDias.longValue());

        return calcularFechasRecursivas(
                varNEquipo,
                varNPozos,
                varEntrada,
                dateInfo.anio,
                dateInfo.diasPerf,
                dateInfo.diasTerm,
                varNDias,
                varFechaInicio,
                varFechaTermino);
    }

    @Transactional
    @CacheResult(cacheName = "tipo-calculo-cache")
    protected String obtenerTipoCalculo(@CacheKey Integer pnIdVersion, @CacheKey Integer pnOportunidadObjetivo) {

        String sql = """
                SELECT DISTINCT o.tipocalculo
                FROM catalogo.oportunidadvw o
                LEFT JOIN catalogo.claveobjetivovw co ON
                    co.idoportunidad = o.idoportunidad
                    AND co.idversion = o.idversion
                WHERE o.idversion = ?
                AND co.idoportunidadobjetivo = ?
                """;

        try {
            return (String) em.createNativeQuery(sql)
                    .setParameter(1, pnIdVersion)
                    .setParameter(2, pnOportunidadObjetivo)
                    .getSingleResult();
        } catch (jakarta.persistence.NoResultException e) {
            Log.error("No tipo calculo found for idVersion=" + pnIdVersion +
                    " and idOportunidadObjetivo=" + pnOportunidadObjetivo);
            throw new IllegalStateException("No se encontró tipo de cálculo para la oportunidad objetivo: " +
                    pnOportunidadObjetivo, e);
        }
    }

    protected Double calcularNumeroPozoArea(
            Integer pnIdVersion,
            Integer pnOportunidadObjetivo,
            Double pnArea) {
        NumeroPozoAreaConvencionalResult result = numeroPozoAreaConvencionalService.calcularNumeroPozoAreaConvencional(
                pnIdVersion,
                pnOportunidadObjetivo,
                pnArea);

        return result.getNumPozo();
    }

    private Integer calcularNumeroPozos(
            String tipoCalculo,
            Integer pnIdVersion,
            Integer pnOportunidadObjetivo,
            Double pnCuota,
            Double pnDeclinada,
            Double pnPce,
            Double pnArea) {

        switch (tipoCalculo) {
            case "Volumen":
                List<ProduccionPozos> volumenResults = pozoVolumenService.calcularPozoVolumen(
                        pnIdVersion, pnOportunidadObjetivo, pnCuota, pnDeclinada, pnPce);
                return (int) Math.ceil(volumenResults.get(0).getCvnumpozo());

            case "Area":
                Double numPozo = calcularNumeroPozoArea(pnIdVersion, pnOportunidadObjetivo, pnArea);
                return (int) Math.ceil(numPozo);

            case "Ambos":
                List<ProduccionPozos> volResults = pozoVolumenService.calcularPozoVolumen(
                        pnIdVersion, pnOportunidadObjetivo, pnCuota, pnDeclinada, pnPce);
                Double pozoVolumen = Math.ceil(volResults.get(0).getCvnumpozo());

                Double pozoArea = Math.ceil(calcularNumeroPozoArea(pnIdVersion, pnOportunidadObjetivo, pnArea));
                return (int) Math.min(pozoVolumen, pozoArea);

            default:
                throw new IllegalArgumentException("Tipo calculo no reconocido: " + tipoCalculo);
        }
    }

    private static class QueryDateInfo {
        String anio;
        Double diasPerf;
        Double diasTerm;

        QueryDateInfo(String anio, Double diasPerf, Double diasTerm) {
            this.anio = anio;
            this.diasPerf = diasPerf;
            this.diasTerm = diasTerm;
        }
    }

    @Transactional
    @CacheResult(cacheName = "fecha-info-cache")
    protected QueryDateInfo obtenerInfoFechas(@CacheKey Integer pnIdVersion, @CacheKey Integer pnOportunidadObjetivo) {
        String sql = """
                    SELECT fechainicio, duracionperfpozodesarrollo, duraciontermpozodesarrollo
                    FROM catalogo.claveobjetivovw
                    WHERE idoportunidadobjetivo = :idOportunidadObjetivo
                    AND idversion = :idVersion
                    LIMIT 1
                """;

        Object[] result = (Object[]) em.createNativeQuery(sql)
                .setParameter("idOportunidadObjetivo", pnOportunidadObjetivo)
                .setParameter("idVersion", pnIdVersion)
                .getSingleResult();

        return new QueryDateInfo(
                result[0].toString(),
                ((Number) result[1]).doubleValue(),
                ((Number) result[2]).doubleValue());
    }

    @Transactional
    @CacheResult(cacheName = "fecha-inicio-base-cache")
    protected LocalDateTime obtenerFechaInicio(@CacheKey Integer pnOportunidadObjetivo) {
        String sql = """
                    SELECT CAST(CONCAT('01/01/', fechainicio) as timestamp)
                    FROM catalogo.claveobjetivovw
                    WHERE idoportunidadobjetivo = :idOportunidadObjetivo
                    LIMIT 1
                """;

        return ((java.sql.Timestamp) em.createNativeQuery(sql)
                .setParameter("idOportunidadObjetivo", pnOportunidadObjetivo)
                .getSingleResult())
                .toLocalDateTime();
    }

    private List<FechaInicioResult> calcularFechasRecursivas(
            Integer varNEquipo,
            Integer varNPozos,
            Integer varEntrada,
            String varAnio,
            Double varNDiasPerf,
            Double varNDiasTerm,
            Double varNDias,
            LocalDateTime varFechaInicio,
            LocalDateTime varFechaTermino) {

        List<FechaInicioResult> results = new ArrayList<>();

        // Initial record (matching the first part of CTE)
        agregarResultado(
                results,
                varFechaInicio,
                varFechaTermino,
                varNEquipo,
                varNEquipo, // Initial npozo equals equipo
                varEntrada,
                1,
                varNDiasPerf,
                varNDiasTerm,
                varNDias);

        // Recursive part (matching UNION ALL in CTE)
        LocalDateTime currentFechaInicio = varFechaTermino;
        LocalDateTime currentFechaTermino;

        for (int idCte = 2; idCte <= varEntrada; idCte++) {
            currentFechaTermino = currentFechaInicio.plusDays(varNDias.longValue());

            // Exactly matching the original SQL CASE logic
            int npozo = (idCte == varEntrada) ? varNPozos : (varNEquipo * idCte);

            agregarResultado(
                    results,
                    currentFechaInicio,
                    currentFechaTermino,
                    varNEquipo,
                    npozo,
                    varEntrada,
                    idCte,
                    varNDiasPerf,
                    varNDiasTerm,
                    varNDias);

            currentFechaInicio = currentFechaTermino;
        }

        return results;
    }

    private void agregarResultado(
            List<FechaInicioResult> results,
            LocalDateTime fechaInicio,
            LocalDateTime fechaTermino,
            int equipo,
            int npozo,
            int entrada,
            int idCte,
            double diasPerf,
            double diasTerm,
            double dias) {

        double monthFraction = calcularFraccionMes(fechaTermino);
        LocalDateTime fechaEntrada;
        double mes, anio;

        if (monthFraction > 0.5) {
            fechaEntrada = fechaTermino.plusMonths(1);
            mes = fechaEntrada.getMonthValue();
            anio = fechaEntrada.getYear();
        } else {
            fechaEntrada = fechaTermino;
            mes = fechaTermino.getMonthValue();
            anio = fechaTermino.getYear();
        }

        results.add(new FechaInicioResult(
                fechaEntrada,
                mes,
                anio,
                fechaInicio,
                fechaTermino,
                equipo,
                npozo,
                entrada,
                diasPerf,
                diasTerm,
                dias,
                idCte));
    }

    private double calcularFraccionMes(LocalDateTime date) {
        LocalDateTime monthStart = date.withDayOfMonth(1);
        LocalDateTime nextMonthStart = monthStart.plusMonths(1);
        LocalDateTime monthEnd = nextMonthStart.minusDays(1);

        long daysInMonth = ChronoUnit.DAYS.between(monthStart, nextMonthStart);
        long daysFromMonthStart = ChronoUnit.DAYS.between(monthStart, date);

        return (double) daysFromMonthStart / daysInMonth;
    }
}