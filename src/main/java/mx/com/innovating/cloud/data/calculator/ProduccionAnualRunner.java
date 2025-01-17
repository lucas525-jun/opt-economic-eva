package mx.com.innovating.cloud.data.calculator;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import io.quarkus.logging.Log;

import io.quarkus.runtime.Quarkus;
import io.quarkus.runtime.QuarkusApplication;
import io.quarkus.runtime.annotations.QuarkusMain;
import java.sql.Connection;
import java.util.List;
import java.util.ArrayList;

// import mx.com.innovating.cloud.data.models.FechaInicioResult;
import mx.com.innovating.cloud.data.models.*;
import mx.com.innovating.cloud.data.models.ProduccionTotalMmbpce;

import mx.com.innovating.cloud.data.calculator.ProduccionAnualService;
import mx.com.innovating.cloud.data.calculator.EscaleraProduccionService;
// import mx.com.innovating.cloud.data.calculator.FechaInicioService;
// import mx.com.innovating.cloud.data.calculator.PozoVolumenService;
import mx.com.innovating.cloud.data.repository.DataBaseConnectorRepository;

// @QuarkusMain
public class ProduccionAnualRunner implements QuarkusApplication {

    @Inject
    ProduccionAnualService service;

    @Inject
    EscaleraProduccionService escaleraProduccionService;

    @Inject
    DataBaseConnectorRepository dataBaseConnectorRepository;

    // @Inject
    // PozoVolumenService pozoVolumenService;

    public static void main(String... args) {
        Quarkus.run(ProduccionAnualRunner.class, args);
    }

    @Override
    public int run(String... args) {
        int pnidversion = 112;
        int pnoportunidadobjetivo = 3164;
        double pncuota = 3.3884525419684497;
        double pndeclinada = 14.973997848271287;
        double pnpce = 36.148568808209205;
        double pnarea = 2.1133959936268787;
        // List<ProduccionTotalMmbpce> results = new ArrayList<>();
        // List<EscaleraProduccion> results = new ArrayList<>();
        // List<ProduccionPozos> results = new ArrayList<>();
        Log.info("================started");
        try {
            // results = escaleraProduccionService.calculateEscaleraProduccion(
            // results = fechaInicioService.calcularFechaInicio(

            // results = pozoVolumenService.calcularPozoVolumen(
            ProduccionTotalMmbpce results = dataBaseConnectorRepository.getProduccionTotalMmbpce(
                    pnoportunidadobjetivo,
                    pnidversion,
                    pncuota,
                    pndeclinada,
                    pnpce,
                    pnarea);
            Log.info("================ended");
            // results.stream()
            // .limit(10)
            // .forEach(row -> Log.info(row.toString()));
            Log.info("Production Total Mmbpce: " + results.getProduccionTotalMmbpce());

            // Log.info("Total number of results: " + results.size());

            return 0;
        } catch (Exception e) {
            e.printStackTrace();
            return 1;
        }
    }
}