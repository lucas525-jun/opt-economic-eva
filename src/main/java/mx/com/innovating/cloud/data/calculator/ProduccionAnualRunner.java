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
import mx.com.innovating.cloud.data.calculator.DeclinaExpOportunidadService;
import mx.com.innovating.cloud.data.calculator.FechaInicioService;
import mx.com.innovating.cloud.data.calculator.PozoVolumenService;

import mx.com.innovating.cloud.data.repository.DataBaseConnectorRepository;

@QuarkusMain
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

    public static void main(String... args) {
        Quarkus.run(ProduccionAnualRunner.class, args);
    }

    @Override
    public int run(String... args) {

        // cacheManager.getCacheNames().forEach(cacheName ->
        // cacheManager.getCache(cacheName).get().invalidateAll());
        int pnidversion = 101;
        int pnoportunidadobjetivo = 3153;
        double pncuota = 6.40411740557325;
        double pndeclinada = 10.0890784038171;
        double pnpce = 5.81635999830272;
        double pnarea = 4.0561533216062;
        // List<ProduccionTotalMmbpce> results = new ArrayList<>();
        // List<EscaleraProduccion> results = new ArrayList<>();
        // List<ProduccionPozos> results = new ArrayList<>();
        // List<FechaInicioResult> results = new ArrayList<>();
        List<VectorProduccion> results = new ArrayList<>();

        Log.info("================started");
        try {
            // for (int i = 0; i < 20; i++) {
            long startTime = System.nanoTime();

            // results = escaleraProduccion.calcularDeclinaExpoportunidad(
            // results = fechaInicioService.calcularFechaInicio(
            // results = pozoVolumenService.calcularPozoVolumen(
            // results = dataBaseConnectorRepository.getPozosPerforados(
            results = dataBaseConnectorRepository.getVectorProduccion(
                    pnoportunidadobjetivo,
                    pnidversion,
                    pncuota,
                    pndeclinada,
                    // pnpce);
                    pnpce,
                    pnarea);
            Log.info("================ended");

            long endTime = System.nanoTime();
            long duration = endTime - startTime;
            // Log.info("Execution " + (i + 1) + " took " + duration + " ns");

            // }
            results.stream()
                    // .limit(10)
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