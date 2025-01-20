package mx.com.innovating.cloud.data.calculator;

import io.quarkus.cache.CacheKey;
import io.quarkus.cache.CacheResult;
import io.quarkus.logging.Log;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.persistence.EntityManager;
import jakarta.transaction.Transactional;
import mx.com.innovating.cloud.data.models.DeclinaExpoportunidadResult;
import mx.com.innovating.cloud.data.models.EscaleraProduccion;
import mx.com.innovating.cloud.data.models.FechaInicioResult;
import mx.com.innovating.cloud.data.models.ProduccionPozos;
import mx.com.innovating.cloud.data.calculator.DeclinaExpOportunidadService;
import mx.com.innovating.cloud.data.calculator.FechaInicioService;
import mx.com.innovating.cloud.data.calculator.PozoVolumenService;

import java.time.LocalDate;
import java.time.YearMonth;
import java.sql.Date;
import java.util.*;
import java.util.stream.Collectors;

@ApplicationScoped
public class EscaleraProduccionService {
    @Inject
    EntityManager em;

    @Inject
    DeclinaExpOportunidadService declinaExpService;

    @Inject
    FechaInicioService fechaInicioService;

    @Inject
    PozoVolumenService pozoVolumenService;

    private static class DeclinadaPozo {
        private final int idsecuencia;
        private final int idoportunidadobjetivo;
        private final double perfil;
        private final double recurso;
        private final double lndeclinada;

        DeclinadaPozo(int idsecuencia, int idoportunidadobjetivo, double perfil,
                double recurso, double lndeclinada) {
            this.idsecuencia = idsecuencia;
            this.idoportunidadobjetivo = idoportunidadobjetivo;
            this.perfil = perfil;
            this.recurso = recurso;
            this.lndeclinada = lndeclinada;
        }
    }

    private static class SecuenciaPozo {
        private final int idsec;
        private final int idPozo;
        private final String pozo;

        SecuenciaPozo(int idsec, int idPozo, String pozo) {
            this.idsec = idsec;
            this.idPozo = idPozo;
            this.pozo = pozo;
        }
    }

    private static class SecuenciaMes {
        private final int idsecuencia;
        private final int mes;
        private final String anio;
        private final int npozos;
        private final int diasmes;
        private final Date fecha;

        SecuenciaMes(int idsecuencia, int mes, String anio, int npozos, int diasmes, Date fecha) {
            this.idsecuencia = idsecuencia;
            this.mes = mes;
            this.anio = anio;
            this.npozos = npozos;
            this.diasmes = diasmes;
            this.fecha = fecha;
        }
    }

    @Transactional
    @CacheResult(cacheName = "escalera-produccion-cache")
    public List<EscaleraProduccion> calculateEscaleraProduccion(
            @CacheKey int pnidversion,
            @CacheKey int pnoportunidadobjetivo,
            @CacheKey double pncuota,
            @CacheKey double pndeclinada,
            @CacheKey double pnpce,
            @CacheKey double pnarea) {

        // Fetch declinada results
        List<DeclinaExpoportunidadResult> declinaResults = declinaExpService.calcularDeclinaExpoportunidad(
                pnidversion, pnoportunidadobjetivo, pncuota, pndeclinada, pnpce);

        // Convert declinada results
        List<DeclinadaPozo> declinadaPozo = declinaResults.stream()
                .map(r -> new DeclinadaPozo(
                        r.getVidSecuencia(),
                        r.getVidOportunidadObjetivo(),
                        r.getVexpDeclinada(),
                        r.getVrecurso(),
                        r.getVlnDeclinada()))
                .collect(Collectors.toList());

        // Fetch fecha inicio results
        List<FechaInicioResult> fechaInicioResults = fechaInicioService.calcularFechaInicio(
                pnidversion, pnoportunidadobjetivo, pncuota, pndeclinada, pnpce, pnarea);

        // Calculate parameters similar to SPP
        int nmes = declinadaPozo.size() - 2;
        List<EscaleraProduccion> results = new ArrayList<>();
        int consecutiveId = 1;
        int pi = 1;

        // First pass: Create all well sequences to get max idsec (z)
        List<SecuenciaPozo> secuenciaPozos = new ArrayList<>();

        for (FechaInicioResult entrada : fechaInicioResults) {
            int pf = entrada.getNPozos();
            for (int idPozo = pi; idPozo <= pf; idPozo++) {
                String pozoName = "POZO " + idPozo;
                secuenciaPozos.add(new SecuenciaPozo(secuenciaPozos.size() + 1, idPozo, pozoName));
            }
            pi = pf + 1;
        }

        // Get z (max idsec) and remainder
        int z = secuenciaPozos.size();
        ProduccionPozos pv = pozoVolumenService.calcularPozoVolumen(
                pnidversion, pnoportunidadobjetivo, pncuota, pndeclinada, pnpce).get(0);
        double r = pv.getCvnumpozo() - Math.floor(pv.getCvnumpozo());

        // Reset for main processing
        pi = 1;

        for (int x = 0; x < fechaInicioResults.size(); x++) {
            FechaInicioResult currentEntrada = fechaInicioResults.get(x);
            LocalDate varFecha = currentEntrada.getFechaEntrada().toLocalDate();
            int pf = currentEntrada.getNPozos();

            for (int idPozo = pi; idPozo <= pf; idPozo++) {
                String pozoName = "POZO " + idPozo;
                int currentSeqId = secuenciaPozos.get(idPozo - 1).idsec;

                for (int mesIndex = 0; mesIndex <= nmes; mesIndex++) {
                    LocalDate currentDate = varFecha.plusMonths(mesIndex);
                    YearMonth yearMonth = YearMonth.from(currentDate);
                    DeclinadaPozo dp = declinadaPozo.get(mesIndex);

                    // Exactly match SPP's CASE logic
                    double produccion;
                    if (currentSeqId == z && r > 0) {
                        produccion = ((dp.perfil * yearMonth.lengthOfMonth()) / 1000.0) * r;
                    } else {
                        produccion = (dp.perfil * yearMonth.lengthOfMonth()) / 1000.0;
                    }

                    results.add(new EscaleraProduccion(
                            consecutiveId++,
                            mesIndex + 1,
                            idPozo,
                            pozoName,
                            dp.perfil,
                            yearMonth.lengthOfMonth(),
                            dp.idoportunidadobjetivo,
                            String.valueOf(currentDate.getYear()),
                            currentDate.getMonthValue(),
                            produccion,
                            Date.valueOf(currentDate)));
                }
            }
            pi = pf + 1;
        }

        return results.stream()
                .sorted(Comparator
                        .comparing(EscaleraProduccion::getAnio)
                        .thenComparing(EscaleraProduccion::getMes)
                        .thenComparing(EscaleraProduccion::getIdpozo))
                .collect(Collectors.toList());
    }
}