package mx.com.innovating.cloud.data.calculator;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import io.quarkus.cache.CacheManager;
import io.quarkus.logging.Log;

import io.quarkus.runtime.Quarkus;
import io.quarkus.runtime.QuarkusApplication;
import io.quarkus.runtime.annotations.QuarkusMain;
import java.sql.Connection;
import java.util.List;
import java.util.ArrayList;

import mx.com.innovating.cloud.data.models.*;
import mx.com.innovating.cloud.data.calculator.ProduccionAnualService;
import mx.com.innovating.cloud.data.calculator.EscaleraProduccionService;
import mx.com.innovating.cloud.data.calculator.EspesorNetoService.EspesorNetoResult;
import mx.com.innovating.cloud.data.calculator.DeclinaExpOportunidadService;
import mx.com.innovating.cloud.data.calculator.FechaInicioService;
import mx.com.innovating.cloud.data.calculator.PozoVolumenService;
import mx.com.innovating.cloud.data.calculator.NumeroPozoporAreaService;
import mx.com.innovating.cloud.data.calculator.NumeroPozoPorVolumenService;
import mx.com.innovating.cloud.data.calculator.FechaAnioService;
import mx.com.innovating.cloud.data.calculator.FechaAnioService.FechaAnioResult;
import mx.com.innovating.cloud.data.calculator.NumeroPozoAreaConvencionalService;
import mx.com.innovating.cloud.data.repository.DataBaseConnectorRepository;

// @QuarkusMain
public class ProduccionAnualRunner implements QuarkusApplication {
    // @Inject
    // CacheConfig cacheConfig;

    @Inject
    ProduccionAnualService service;

    @Inject
    EscaleraProduccionService escaleraProduccionService;

    @Inject
    DataBaseConnectorRepository dataBaseConnectorRepository;

    @Inject
    PozoVolumenService pozoVolumenService;

    @Inject
    FechaInicioService fechaInicioService;

    @Inject
    DeclinaExpOportunidadService declinaExpOportunidadService;

    @Inject
    NumeroPozoporAreaService numeroPozoporAreaService;

    @Inject
    NumeroPozoPorVolumenService numeroPozoPorVolumenService;

    @Inject
    FechaAnioService fechaAnioService;

    @Inject
    HiperbolicoAnualService hiperbolicoAnualService;

    @Inject
    EspesorNetoService espesorNetoService;

    @Inject
    EscaleraProduccionNdService escaleraProduccionNdService;

    public static void main(String... args) {
        Quarkus.run(ProduccionAnualRunner.class, args);
    }

    @Override
    public int run(String... args) {

        // int pnidversion = 101;
        // int pnoportunidadobjetivo = 3153;
        // double pncuota = 6.40411740557325;
        // double pndeclinada = 10.0890784038171;
        // double pnpce = 5.81635999830272;
        // double pnarea = 4.0561533216062;

        Integer pnidversion = 1;
        Integer pnoportunidadobjetivo = 1852;
        Integer pntipovalor = 2;
        Integer pnmeses = 240;
        double pnaleatorio = 0.339936256408691;

        Integer percentil = 66;
        Log.info("================started");
        try {
            // for (int i = 0; i < 20; i++) {
            long startTime = System.nanoTime();

            List<EscaleraProduccionNdService.EscaleraProduccionNdResult> results = escaleraProduccionNdService
                    .spcEscaleraProduccionNd(
                            pnidversion,
                            pnoportunidadobjetivo,
                            pntipovalor,
                            pnmeses,
                            pnaleatorio);

            Log.info("================ended");
            long endTime = System.nanoTime();
            long duration = endTime - startTime;
            // Log.info("Execution " + (i + 1) + " took " + duration + " ns");

            // }
            results.stream()
                    .limit(108)
                    .forEach(row -> Log.info(row.toString()));

            Log.info("Total number of results: " + results.size());

            return 0;
        } catch (

        Exception e) {
            e.printStackTrace();
            return 1;
        }
    }
}