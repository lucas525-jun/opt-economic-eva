package com.pemex.pep.seemop.data.calculator;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

@ApplicationScoped
public class EscaleraProduccionNdService {

        @Inject
        FechaAnioService fechaAnioService;

        @Inject
        HiperbolicoAnualService hiperbolicoAnualService;

        @Data
        @AllArgsConstructor
        @NoArgsConstructor
        public static class EscaleraProduccionNdResult {
                private Integer vvidsecuencia;
                private Integer vidpozo;
                private String vpozo;
                private Integer vvidoportunidadobjetivo;
                private String vanio;
                private Double vproduccion;
                private LocalDate vfecha;
        }

        @Data
        @AllArgsConstructor
        @NoArgsConstructor
        private static class ProduccionPozo {
                private Integer idsecuencia;
                private String anio;
                private Integer idoportunidadobjetivo;
                private Double produccion;
        }

        @Data
        @AllArgsConstructor
        @NoArgsConstructor
        private static class EntradaPozo {
                private Integer idsecuencia;
                private String anio;
                private Integer npozos;
                private Integer nentrada;
        }

        @Data
        @AllArgsConstructor
        @NoArgsConstructor
        private static class SecuenciaAnio {
                private Integer idsecuencia;
                private String anio;
                private Integer npozos;
                private LocalDate fecha;
        }

        @Data
        @AllArgsConstructor
        @NoArgsConstructor
        private static class SecuenciaPozo {
                private Integer idsec;
                private Integer idPozo;
                private String pozo;
        }

        @Data
        @AllArgsConstructor
        @NoArgsConstructor
        private static class EscaleraPozo {
                private Integer idconsecutivo;
                private Integer idsecuencia;
                private String anio;
                private String pozo;
                private Integer idpozo;
                private LocalDate fecha;
        }

        @Data
        @AllArgsConstructor
        @NoArgsConstructor
        private static class EscaleraProduccion {
                private Integer idconsecutivo;
                private Integer idsecuencia;
                private Integer idpozo;
                private String pozo;
                private Integer idoportunidadobjetivo;
                private String anio;
                private LocalDate fecha;
                private Double produccion;
        }

        public List<EscaleraProduccionNdResult> spcEscaleraProduccionNd(
                        final Integer pnidversion,
                        final Integer pnoportunidadobjetivo,
                        final Integer pntipovalor,
                        final Integer pnmeses,
                        final Double pnaleatorio,
                        Integer percentil) {

                // 1. Initialize production data exactly as SPP
                AtomicInteger counter = new AtomicInteger(1);

                final List<ProduccionPozo> produccionPozoTable = hiperbolicoAnualService.sppHiperbolicoAnual(
                                pnidversion, pnoportunidadobjetivo, pntipovalor, pnmeses)
                                .stream()
                                .map(result -> {
                                        int panio = Integer.parseInt(result.getPanio()); // Convert String to int
                                        return new ProduccionPozo(
                                                        counter.getAndIncrement(),
                                                        result.getPanio(),
                                                        pnoportunidadobjetivo,
                                                        result.getPpmbpce());
                                })
                                .collect(Collectors.toList());

                // 2. Get entry data exactly as SPP
                final List<EntradaPozo> entradaPozoTable = fechaAnioService.spcFechaAnio(
                                pnidversion, pnoportunidadobjetivo, pntipovalor, pnmeses, pnaleatorio, percentil)
                                .stream()
                                .map(result -> new EntradaPozo(
                                                result.getVid(),
                                                result.getAnio(),
                                                result.getNpozo(),
                                                result.getVnpozo()))
                                .collect(Collectors.toList());
                // 4. Initialize our working tables
                final List<SecuenciaPozo> secuenciaPozoTable = new ArrayList<>();
                final List<SecuenciaAnio> secuenciaAnioTable = new ArrayList<>();
                // 3. Initialize variables exactly as SPP
                final int nmes = pnmeses - 2;
                final int veces = entradaPozoTable.stream()
                                .mapToInt(EntradaPozo::getIdsecuencia) // old sp : SELECT MAX(nentrada) FROM
                                                                       // entradapozo, but
                                                                       // idsecuencia is correct
                                .max()
                                .orElse(0);
                final int varpozoant = 0;
                final AtomicInteger pi = new AtomicInteger(1); // Use AtomicInteger for mutability

                // 5. Using IntStream instead of for loop to maintain final variable requirement

                IntStream.rangeClosed(1, veces).forEach(x -> {
                        // Get current entry for this iteration
                        final EntradaPozo currentEntry = entradaPozoTable.stream()
                                        .filter(e -> e.getIdsecuencia() == x)
                                        .findFirst()
                                        .orElseThrow(() -> new IllegalStateException(
                                                        "No entry found for sequence " + x));

                        // Calculate values for this iteration (like SQL variables)
                        final LocalDate varfecha = LocalDate.of(Integer.parseInt(currentEntry.getAnio()), 1, 1);
                        final int varpozo = currentEntry.getNpozos();
                        final int pf = currentEntry.getNpozos();

                        // Generate wells - equivalent to first INSERT in SQL
                        IntStream.rangeClosed(pi.get(), pf).forEach(np -> secuenciaPozoTable.add(new SecuenciaPozo(
                                        np,
                                        varpozo,
                                        "POZO " + np)));

                        // Generate years for this iteration - equivalent to second INSERT with
                        // ROW_NUMBER()
                        List<SecuenciaAnio> iterationYears = new ArrayList<>();
                        IntStream.rangeClosed(0, nmes / 12).forEach(yearOffset -> {
                                LocalDate fecha = varfecha.plusYears(yearOffset);
                                iterationYears.add(new SecuenciaAnio(
                                                null,
                                                String.valueOf(fecha.getYear()),
                                                pf,
                                                fecha));
                        });

                        // Sort and assign sequence numbers within this iteration
                        AtomicInteger iterationCounter = new AtomicInteger(1);
                        iterationYears.stream()
                                        .sorted(Comparator.comparing(SecuenciaAnio::getFecha))
                                        .forEach(entry -> {
                                                entry.setIdsecuencia(iterationCounter.getAndIncrement());
                                                secuenciaAnioTable.add(entry);
                                        });

                        // Update pi for next iteration
                        pi.set(pf + 1);
                });

                // 6. Create escalera pozo exactly as SPP
                final List<EscaleraPozo> escaleraPozoTable = secuenciaAnioTable.stream()
                                .flatMap(sm -> secuenciaPozoTable.stream()
                                                .filter(sp -> sp.getIdPozo() == sm.getNpozos()) // Exact match as per
                                                                                                // SQL JOIN condition
                                                .map(sp -> new EscaleraPozo(
                                                                null,
                                                                sm.getIdsecuencia(),
                                                                sm.getAnio(),
                                                                sp.getPozo(),
                                                                sp.getIdsec(),
                                                                sm.getFecha())))
                                .sorted(Comparator
                                                .comparing(EscaleraPozo::getAnio)
                                                .thenComparing(ep -> ep.getPozo())
                                                .thenComparing(EscaleraPozo::getIdsecuencia))
                                .collect(Collectors.toList());

                // 7. Create escalera produccion exactly as SPP
                final List<EscaleraProduccion> escaleraProduccionTable = new ArrayList<>();
                for (final EscaleraPozo ep : escaleraPozoTable) {
                        final ProduccionPozo matchingProduction = produccionPozoTable.stream()
                                        .filter(pp -> pp.getIdsecuencia().equals(ep.getIdsecuencia()))
                                        .findFirst()
                                        .orElseThrow(
                                                        () -> new IllegalStateException(
                                                                        "No production data for sequence "
                                                                                        + ep.getIdsecuencia()));

                        escaleraProduccionTable.add(new EscaleraProduccion(
                                        null,
                                        ep.getIdsecuencia(),
                                        ep.getIdpozo(),
                                        ep.getPozo(),
                                        pnoportunidadobjetivo,
                                        ep.getAnio(),
                                        ep.getFecha(),
                                        matchingProduction.getProduccion()));
                }

                // 8. Final return exactly as SPP
                return escaleraProduccionTable.stream()
                                .map(ep -> new EscaleraProduccionNdResult(
                                                ep.getIdsecuencia(),
                                                ep.getIdpozo(),
                                                ep.getPozo(),
                                                ep.getIdoportunidadobjetivo(),
                                                ep.getAnio(),
                                                ep.getProduccion(),
                                                ep.getFecha()))
                                .sorted(Comparator
                                                .comparing(EscaleraProduccionNdResult::getVanio)
                                                .thenComparing(EscaleraProduccionNdResult::getVidpozo)
                                                .thenComparing(EscaleraProduccionNdResult::getVpozo))
                                .collect(Collectors.toList());
        }

        public List<EscaleraProduccionNdResult> spcEscaleraProduccionNd(
                        final Integer pnidversion,
                        final Integer pnoportunidadobjetivo,
                        final Integer pntipovalor,
                        final Integer pnmeses,
                        final Double pnaleatorio) {
                Integer percentil = 66;
                return spcEscaleraProduccionNd(
                                pnidversion,
                                pnoportunidadobjetivo,
                                pntipovalor,
                                pnmeses,
                                pnaleatorio,
                                percentil);
        }
}