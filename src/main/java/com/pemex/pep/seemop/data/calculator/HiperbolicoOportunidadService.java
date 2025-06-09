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

import java.time.LocalDate;
import java.time.YearMonth;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;

@ApplicationScoped
public class HiperbolicoOportunidadService {

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    public static class HiperbolicoResult {
        private Integer vidsecuencia;
        private Integer vanio;
        private Integer vmes;
        private Integer vidtipohidrocarburo;
        private String vhidrocarburo;
        private Integer vidoportunidadobjetivo;
        private Integer vidoportunidad;
        private String voportunidad;
        private String vclaveoportunidad;
        private String vclaveobjetivo;
        private Double vhiperbolico;
        private Double vporcentaje;
        private Double vporcentajeajustado;
        private Double vhiperbolicoajustado;
        private Integer vdiasmes;
        private Double vproduccion;
        private Double vgasto;
        private Double vaceite;
        private Double vgas;
        private Double vcondensado;
        private Double vpmbpce;
    }

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    private static class Hiperbolicomes {
        private Integer idoportunidadobjetivo;
        private Integer idoportunidad;
        private String oportunidad;
        private Integer idclaveoportunidad;
        private String claveoportunidad;
        private Integer idclaveobjetivo;
        private String claveobjetivo;
        private String fecha;
        private LocalDate mesinicio;
        private Integer meses;
        private LocalDate mesperf;
        private Double gastoinicial;
        private Double hiperbolico;
        private Double b;
        private Double di;
        private Double rga;
        private Double rsimedio;
        private Double frcmedio;
        private Double dimen;
        private String anio;
        private Integer dias;
        private Integer idtipohidrocarburo;
        private String hidrocarburo;
        private Integer idversion;
    }

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    private static class Declinadahiperbolico {
        private Integer idtemhip;
        private Integer idoportunidadobjetivo;
        private LocalDate fecha;
        private Integer fechainicio;
        private Integer meses;
        private Integer nmes;
        private Integer idversion;
        private String hidrocarburo;
        private Double hiperbolico;
        private Double hiperbolicoaceite;
        private Double hiperbolicogas;
        private Double hiperbolicocondensado;
        private Double porcentaje;
        private Double b;
        private Double di;
        private Integer anio;
        private Integer dias;
        private Integer diasmes;
        private Double gastoinicial;
        private Double hiperbolicoajustada;
        private Double porcentajeajustada;
        private Double aceite;
        private Double gas;
        private Double condensado;
        private Double pmbpce;
    }

    @Inject
    EntityManager em;

    @Transactional
    @CacheResult(cacheName = "datos-iniciales")
    protected List<Object[]> getDatosIniciales(@CacheKey Integer pnidversion,
            @CacheKey Integer pnoportunidadobjetivo,
            @CacheKey Integer pntipovalor) {
        String w_sql = """
                    SELECT
                        ga.idoportunidadobjetivo,
                        de.idoportunidad,
                        de.oportunidad,
                        de.claveoportunidad,
                        de.claveobjetivo,
                        CONCAT('01/01/',de.fechainicio) as fecha,
                        ga.gasto,
                        de.b,
                        de.di,
                        de.rga,
                        de.rsimedio,
                        de.frcmedio,
                        de.declinacionmensual,
                        de.fechainicio,
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
                    WHERE de.idversion = :pnidversion
                        AND de.idoportunidadobjetivo = :pnoportunidadobjetivo
                        AND de.idtipovalor = :pntipovalor
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

        return em.createNativeQuery(w_sql)
                .setParameter("pnidversion", pnidversion)
                .setParameter("pnoportunidadobjetivo", pnoportunidadobjetivo)
                .setParameter("pntipovalor", pntipovalor)
                .getResultList();
    }

    public List<HiperbolicoResult> sppHiperbolicoOportunidad(Integer pnidversion,
            Integer pnoportunidadobjetivo,
            Integer pntipovalor,
            Integer pnmeses) {
        // Get initial data
        List<Object[]> datosIniciales = getDatosIniciales(pnidversion, pnoportunidadobjetivo, pntipovalor);
        if (datosIniciales.isEmpty()) {
            return new ArrayList<>();
        }
        Object[] datos = datosIniciales.get(0);

        // Initialize variables exactly as SPP
        List<Hiperbolicomes> hiperbolicomes = new ArrayList<>();
        List<Declinadahiperbolico> declinadahiperbolico = new ArrayList<>();

        // First temp table population exactly as SPP
        Hiperbolicomes hipMes = new Hiperbolicomes();
        hipMes.setIdoportunidadobjetivo((Integer) datos[0]);
        hipMes.setIdoportunidad((Integer) datos[1]);
        hipMes.setOportunidad((String) datos[2]);
        hipMes.setClaveoportunidad((String) datos[3]);
        hipMes.setClaveobjetivo((String) datos[4]);
        hipMes.setFecha((String) datos[5]);
        Double w_gasto = ((Number) datos[6]).doubleValue();
        Double w_b = ((Number) datos[7]).doubleValue();
        Double w_di = ((Number) datos[8]).doubleValue();
        hipMes.setGastoinicial(w_gasto);
        hipMes.setMeses(pnmeses);
        hipMes.setHiperbolico(w_gasto / Math.pow(1 + w_b * w_di * 0, 1 / w_b));
        hipMes.setB(w_b);
        hipMes.setDi(w_di);
        hipMes.setRga(((Number) datos[9]).doubleValue());
        hipMes.setRsimedio(((Number) datos[10]).doubleValue());
        hipMes.setFrcmedio(((Number) datos[11]).doubleValue());
        hipMes.setDimen(((Number) datos[12]).doubleValue());
        hipMes.setAnio((String) datos[13]);
        hipMes.setDias(0);
        hipMes.setIdtipohidrocarburo((Integer) datos[14]);
        hipMes.setHidrocarburo((String) datos[15]);
        hipMes.setIdversion((Integer) datos[16]);
        hiperbolicomes.add(hipMes);

        LocalDate mesinicio = LocalDate.parse(hipMes.getFecha(),
                DateTimeFormatter.ofPattern("dd/MM/yyyy"));
        hipMes.setMesinicio(mesinicio.plusDays(hipMes.getDias()));

        // Calculate total months based on initial date
        LocalDate startDate = mesinicio;
        double monthsFromStart = 0;

        // Second temp table population exactly as SPP
        for (int i = 0; i < pnmeses && monthsFromStart < pnmeses; i++) {
            LocalDate fecha = startDate.plusMonths(i);
            monthsFromStart++;

            Declinadahiperbolico decHip = new Declinadahiperbolico();
            decHip.setIdtemhip(i + 1);
            decHip.setIdoportunidadobjetivo(hipMes.getIdoportunidadobjetivo());
            decHip.setFecha(fecha);
            decHip.setNmes(fecha.getMonthValue());
            decHip.setAnio(fecha.getYear());
            decHip.setDiasmes(YearMonth.from(fecha).lengthOfMonth());
            decHip.setB(hipMes.getB());
            decHip.setDi(hipMes.getDi());
            decHip.setHidrocarburo(hipMes.getHidrocarburo());
            declinadahiperbolico.add(decHip);
        }

        // Loop calculation exactly as SPP
        int w_contador = 1;
        int w_maxorden = pnmeses;
        double w_hiperbolico = hipMes.getGastoinicial();
        double w_hiperbolicoant = w_hiperbolico;
        double w_hiperbolicoajustada = hipMes.getGastoinicial();
        double w_hiperbolicoajustadaant = w_hiperbolicoajustada;

        while (w_contador <= w_maxorden) {
            w_hiperbolico = hipMes.getGastoinicial() /
                    Math.pow(1 + hipMes.getB() * hipMes.getDi() * (w_contador - 1), 1 / hipMes.getB());
            double w_porcentaje = 1 - (w_hiperbolico / w_hiperbolicoant);

            double w_porcentajeajustada;
            if (w_porcentaje > hipMes.getDimen()) {
                w_porcentajeajustada = w_porcentaje;
            } else {
                w_porcentajeajustada = hipMes.getDimen();
            }

            w_hiperbolicoajustada = w_hiperbolicoajustadaant - (w_hiperbolicoajustadaant * w_porcentajeajustada);

            Declinadahiperbolico decHip = declinadahiperbolico.get(w_contador - 1);

            if (w_contador == 1) {
                decHip.setHiperbolico(hipMes.getGastoinicial());
                decHip.setHiperbolicoajustada(w_hiperbolicoajustadaant);
                w_hiperbolicoajustadaant = hipMes.getGastoinicial();
            } else {
                decHip.setHiperbolico(w_hiperbolico);
                decHip.setPorcentaje(w_porcentaje);
                decHip.setPorcentajeajustada(w_porcentajeajustada);
                decHip.setHiperbolicoajustada(w_hiperbolicoajustada);
                w_hiperbolicoajustadaant = w_hiperbolicoajustada;
            }

            w_hiperbolicoant = w_hiperbolico;
            w_contador++;
        }

        for (Declinadahiperbolico decHip : declinadahiperbolico) {
            if (decHip.getHidrocarburo().startsWith("Aceite")) {
                decHip.setHiperbolicoaceite(decHip.getHiperbolicoajustada());
                decHip.setHiperbolicocondensado(0.0);
                decHip.setHiperbolicogas(0.0);
                decHip.setAceite(decHip.getHiperbolicoajustada());
                decHip.setGas(decHip.getHiperbolicoajustada() * hipMes.getRga());
                decHip.setCondensado((decHip.getHiperbolicoajustada() * hipMes.getRga() *
                        hipMes.getFrcmedio()) / 1000);
            } else if (decHip.getHidrocarburo().startsWith("Gas")) {
                decHip.setHiperbolicogas(decHip.getHiperbolicoajustada());
                decHip.setHiperbolicocondensado(0.0);
                decHip.setHiperbolicoaceite(0.0);
                decHip.setAceite(0.0);
                decHip.setGas(decHip.getHiperbolicoajustada());
                decHip.setCondensado((decHip.getHiperbolico() * hipMes.getFrcmedio()) / 1000);
            } else if (decHip.getHidrocarburo().startsWith("Condensado")) {
                decHip.setHiperbolicocondensado(decHip.getHiperbolicoajustada());
                decHip.setHiperbolicogas(0.0);
                decHip.setHiperbolicoaceite(0.0);
                decHip.setAceite(0.0);
                decHip.setGas((1000 * decHip.getHiperbolicoajustada()) / hipMes.getFrcmedio());
                decHip.setCondensado(decHip.getHiperbolicoajustada());
            }

            decHip.setPmbpce(decHip.getAceite() + decHip.getCondensado() + decHip.getGas() / 5);
        }

        // Return results exactly as SPP
        List<HiperbolicoResult> results = new ArrayList<>();
        for (Declinadahiperbolico decHip : declinadahiperbolico) {
            results.add(new HiperbolicoResult(
                    decHip.getIdtemhip(),
                    decHip.getAnio(),
                    decHip.getNmes(),
                    hipMes.getIdtipohidrocarburo(),
                    hipMes.getHidrocarburo(),
                    decHip.getIdoportunidadobjetivo(),
                    hipMes.getIdoportunidad(),
                    hipMes.getOportunidad(),
                    hipMes.getClaveoportunidad(),
                    hipMes.getClaveobjetivo(),
                    decHip.getHiperbolico(),
                    decHip.getPorcentaje(),
                    decHip.getPorcentajeajustada(),
                    decHip.getHiperbolicoajustada(),
                    decHip.getDiasmes(),
                    (decHip.getHiperbolicoajustada() * decHip.getDiasmes()) / 1000,
                    hipMes.getGastoinicial(),
                    decHip.getAceite(),
                    decHip.getGas(),
                    decHip.getCondensado(),
                    decHip.getPmbpce()));
        }

        // Clear temp data structures (equivalent to DROP TABLE in SPP)
        hiperbolicomes.clear();
        declinadahiperbolico.clear();
        return results;
    }
}