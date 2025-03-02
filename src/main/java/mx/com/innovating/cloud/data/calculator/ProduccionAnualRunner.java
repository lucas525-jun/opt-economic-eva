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

import java.io.File;

import java.io.FileOutputStream;
import java.io.IOException;

import java.util.Collections;
import com.fasterxml.jackson.databind.ObjectMapper;

import mx.com.innovating.cloud.data.models.*;
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
    FechaInicioMultiObjetivoService fechaInicioMultiObjetivoService;
    @Inject
    FechaInicioService fechaInicioService;

    @Inject
    EscaleraProduccionMultiService escaleraProduccionMultiService;

    @Inject
    DataBaseConnectorRepository dataBaseConnectorRepository;

    @Inject
    PozoVolumenService pozoVolumenService;

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

        int pnidversion = 291;
        int pnoportunidadobjetivo = 4055;
        double pncuota = 20.1273693914138;
        double pndeclinada = 6.34841831044748;
        double pnpce = 36.8233025088598;
        double pnarea = 3.34068122742337;
        String fecha = null;

        // int pnidversion = 275;
        // int pnoportunidadobjetivo = 3918;
        // double pncuota = 0.476092893;
        // double pndeclinada = 18;
        // double pnpce = 2.155497545;
        // double pnarea = 2.743834229;

        // Integer pnidversion = 1;
        // Integer pnoportunidadobjetivo = 1852;
        // Integer pntipovalor = 2;
        // Integer pnmeses = 240;
        // double pnaleatorio = 0.339936256408691;

        // Integer percentil = 66;
        Log.info("================started");
        try {
            // for (int i = 0; i < 20; i++) {
            long startTime = System.nanoTime();

            // List<EscaleraProduccionMulti> results = dataBaseConnectorRepository
            // .calculateEscaleraProduccionMulti(
            // // pnidversion,
            // // pnoportunidadobjetivo,
            // // pntipovalor,
            // // pnmeses,
            // // pnaleatorio);
            // pnoportunidadobjetivo,
            // pnidversion,
            // pncuota,
            // pndeclinada,
            // pnpce,
            // pnarea,
            // fecha);

            // List<FechaInicioResult> results = fechaInicioMultiObjetivoService
            // .sppFechaInicioMulti(

            List<FechaInicioResult> results = fechaInicioService
                    .calcularFechaInicio(

                            pnidversion,
                            pnoportunidadobjetivo,
                            pncuota,
                            pndeclinada,
                            pnpce,
                            pnarea);
            List<FechaInicioResult> resultsSecond = fechaInicioMultiObjetivoService
                    .sppFechaInicioMulti(

                            pnidversion,
                            pnoportunidadobjetivo,
                            pncuota,
                            pndeclinada,
                            pnpce,
                            pnarea);
            // List<FechaInicioResult> results = escaleraProduccionMultiService
            // .calculateEscaleraProduccionMulti(

            // pnidversion,
            // pnoportunidadobjetivo,
            // pncuota,
            // pndeclinada,
            // pnpce,
            // pnarea,
            // fecha);

            Log.info("================first");
            long endTime = System.nanoTime();
            long duration = endTime - startTime;

            // Log.info("Execution " + (i + 1) + " took " + duration + " ns");

            // }
            results.stream()
                    // .limit(108)
                    .forEach(row -> Log.info(row.toString()));
            Log.info("================second");

            resultsSecond.stream()
                    // .limit(108)
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