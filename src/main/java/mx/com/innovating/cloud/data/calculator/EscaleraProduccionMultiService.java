package mx.com.innovating.cloud.data.calculator;

import io.quarkus.logging.Log;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.persistence.EntityManager;
import mx.com.innovating.cloud.data.models.DeclinaExpoportunidadResult;
import mx.com.innovating.cloud.data.models.FechaInicioResult;
import mx.com.innovating.cloud.data.models.NumeroPozoAreaConvencionalResult;
import mx.com.innovating.cloud.data.models.ProduccionPozos;
import mx.com.innovating.cloud.data.models.EscaleraProduccionMulti;


import java.net.URLDecoder;
import java.time.LocalDate;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.time.YearMonth;
import java.time.format.DateTimeFormatter;
import java.sql.Date;
import java.util.*;
import java.util.stream.Collectors;

@ApplicationScoped
public class EscaleraProduccionMultiService {
    @Inject
    EntityManager em;

    @Inject
    DeclinaExpOportunidadService declinaExpService;

    @Inject
    FechaInicioMultiObjetivoService fechaInicioMultiObjetivoService;

    @Inject
    FechaInicioService fechaInicioService;

    @Inject
    PozoVolumenService pozoVolumenService;

    @Inject
    FactoresCalculatorService factoresCalculatorService;
    
    @Inject
    NumeroPozoAreaConvencionalService numeroPozoAreaConvencionalService;

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

    public List<EscaleraProduccionMulti> calculateEscaleraProduccionMulti(
            int pnidversion,
            int pnoportunidadobjetivo,
            double pncuota,
            double pndeclinada,
            double pnpce,
            double pnarea,
            String fechaString) {

        LocalDateTime fechaParam = null;
        if (fechaString == null || fechaString.equals("unexist")) {
        } else {
            try {
                String decodedFecha = URLDecoder.decode(fechaString, StandardCharsets.UTF_8.toString());
                try {
                    // Try full datetime format
                    DateTimeFormatter fullFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
                    fechaParam = LocalDateTime.parse(decodedFecha, fullFormatter);
                } catch (Exception e1) {
                    try {
                        // Try date-only format
                        DateTimeFormatter dateFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd");
                        LocalDate date = LocalDate.parse(decodedFecha, dateFormatter);
                        fechaParam = date.atStartOfDay();
                    } catch (Exception e2) {
                        // Try dd/MM/yyyy format
                        DateTimeFormatter altFormatter = DateTimeFormatter.ofPattern("dd/MM/yyyy");
                        LocalDate date = LocalDate.parse(decodedFecha, altFormatter);
                        fechaParam = date.atStartOfDay();
                    }
                }
            } catch (Exception e) {
                Log.error("Error parsing date: " + fechaString + ". Using current time.", e);
            }
        }

        List<Object[]> factores = factoresCalculatorService.calcularFactores(pnoportunidadobjetivo);
        double fcaceite = 0.0;
        double fcgas = 0.0;
        double fccondensado = 0.0;

        if (!factores.isEmpty()) {
            Object[] factor = factores.get(0);
            fcaceite = factor[0] != null ? (Double) factor[0] : 0.0;
            fcgas = factor[1] != null ? (Double) factor[1] : 0.0;
            fccondensado = factor[2] != null ? (Double) factor[2] : 0.0;
        }

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
        List<FechaInicioResult> fechaInicioResults;
        if (fechaString == null || fechaString.equals("unexist")) {
            fechaInicioResults = fechaInicioMultiObjetivoService
                    .sppFechaInicioMulti(
                            pnidversion, pnoportunidadobjetivo, pncuota, pndeclinada, pnpce, pnarea);

        } else {
            fechaInicioResults = fechaInicioMultiObjetivoService
                    .sppFechaInicioMulti(
                            pnidversion, pnoportunidadobjetivo, pncuota, pndeclinada, pnpce, pnarea, fechaParam);
        }

        int nmes = declinadaPozo.size() - 2;
        List<EscaleraProduccionMulti> results = new ArrayList<>();
        int consecutiveId = 1;
        int pi = 1;
        
        // Tope si es por area
        Double numeroPozoArea = calcularNumeroPozoArea(pnidversion, pnoportunidadobjetivo, pnarea);
        double prodMaxpp = pnpce / numeroPozoArea;
        String tipoCalculo = fechaInicioService.obtenerTipoCalculo(pnidversion, pnoportunidadobjetivo);
        double rArea = numeroPozoArea - Math.floor(numeroPozoArea);
        double fraccionArea = rArea * prodMaxpp;

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

                if(Objects.equals(tipoCalculo, "Volumen")){
                    for (int mesIndex = 0; mesIndex <= nmes; mesIndex++) {
                        LocalDate currentDate = varFecha.plusMonths(mesIndex);
                        YearMonth yearMonth = YearMonth.from(currentDate);
                        DeclinadaPozo dp = declinadaPozo.get(mesIndex);

                        // Calculate base production
                        double produccion;
                        if (currentSeqId == z && r > 0) {
                            produccion = ((dp.perfil * yearMonth.lengthOfMonth()) / 1000.0) * r;
                        } else {
                            produccion = (dp.perfil * yearMonth.lengthOfMonth()) / 1000.0;
                        }

                        // Calculate derived productions
                        double prodAceite = produccion * fcaceite;
                        double prodGas = produccion * fcgas;
                        double prodCondensado = produccion * fccondensado;

                        results.add(new EscaleraProduccionMulti(
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
                                prodAceite,
                                prodGas,
                                prodCondensado,
                                Date.valueOf(currentDate)));
                    }
                } else {
                    double acum = 0.0;
                    double produccion;
                    // mientras que produccion sea menor a prodMaxpp se hacen los calculos
                    for (int mesIndex = 0; mesIndex <= nmes; mesIndex++) {
                        LocalDate currentDate = varFecha.plusMonths(mesIndex);
                        YearMonth yearMonth = YearMonth.from(currentDate);
                        DeclinadaPozo dp = declinadaPozo.get(mesIndex);

                        if (currentSeqId == z && rArea > 0) {
                            produccion = ((dp.perfil * yearMonth.lengthOfMonth()) / 1000.0) * rArea;
                            if (acum + produccion > fraccionArea) {
                                break;
                            }
                        } else {
                            produccion = ((dp.perfil * yearMonth.lengthOfMonth()) / 1000.0);
                            if (acum + produccion > prodMaxpp) {
                                break;
                            }
                        }

                        acum += produccion;
                        
                        // Calculate derived productions
                        double prodAceite = produccion * fcaceite;
                        double prodGas = produccion * fcgas;
                        double prodCondensado = produccion * fccondensado;

                        results.add(new EscaleraProduccionMulti(
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
                                prodAceite,
                                prodGas,
                                prodCondensado,
                                Date.valueOf(currentDate)
                        ));
                    }
                }
            }
            pi = pf + 1;
        }

        return results.stream()
                .sorted(Comparator
                        .comparing(EscaleraProduccionMulti::getAnio)
                        .thenComparing(EscaleraProduccionMulti::getMes)
                        .thenComparing(EscaleraProduccionMulti::getIdpozo))
                .collect(Collectors.toList());
    }
    
    private Double calcularNumeroPozoArea(
            Integer pnIdVersion,
            Integer pnOportunidadObjetivo,
            Double pnArea) {
        NumeroPozoAreaConvencionalResult result = numeroPozoAreaConvencionalService.calcularNumeroPozoAreaConvencional(
                pnIdVersion,
                pnOportunidadObjetivo,
                pnArea);

        return result.getNumPozo();
    }
    
}