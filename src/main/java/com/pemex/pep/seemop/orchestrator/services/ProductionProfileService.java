package com.pemex.pep.seemop.orchestrator.services;

import com.pemex.pep.seemop.data.models.*;
import com.pemex.pep.seemop.data.models.FactorInversion;
import com.pemex.pep.seemop.data.models.PozosActivos;
import com.pemex.pep.seemop.data.models.ProduccionTotalMmbpce;
import com.pemex.pep.seemop.data.models.VectorProduccion;
import com.pemex.pep.seemop.orchestrator.models.*;
import io.quarkus.logging.Log;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import lombok.extern.slf4j.Slf4j;
import com.pemex.pep.seemop.data.entities.InformacionOportunidad;
import mx.com.innovating.cloud.data.models.*;
import com.pemex.pep.seemop.data.repository.DataBaseConnectorRepository;
import mx.com.innovating.cloud.orchestrator.models.*;
import com.pemex.pep.seemop.orchestrator.utilities.DataProcess;
import com.pemex.pep.seemop.data.calculator.FechaInicioService;

import java.math.BigDecimal;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;

import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

@Slf4j
@ApplicationScoped
public class ProductionProfileService {

        @Inject
        DataBaseConnectorRepository databaseConnectorClient;

        @Inject
        EvaluacionMerger evaluacionMerger;

        
        @Inject
        FechaInicioService fechaInicioService;

        private static ThreadLocal<double[]> produccionTotalMmbpceArrayThreadLocal = new ThreadLocal<>();

        private void initializeInstanceVariables() {
                produccionTotalMmbpceArrayThreadLocal.remove();
        }

        private void initializeArrays(int size) {
                produccionTotalMmbpceArrayThreadLocal.set(new double[size]);
        }

        public EvaluacionResponse getInfoPozosService(
                        int indexNumber,
                        Integer idOportunidadObjetivo,
                        Integer version,
                        double cuota, double declinada, double pce, double area, String fecha, int oldPozosTerminadosValue,
                        Map<String, Object> lastYearPozosProduccion,
                        Map<String, ProduccionDiariaPromedio> lastProduccionDiariaPromedio) {
                double[] produccionTotalMmbpceArray = produccionTotalMmbpceArrayThreadLocal.get();

                Boolean multiFlag = true;

                // log.info("PCE distinto de 0. Ejecutando flujo completo.");

                // log.info(" 1 / 12 - getPozoPerforados");
                InformacionOportunidad oportunity = databaseConnectorClient
                                .getInfoOportunidad(idOportunidadObjetivo);

                // log.info(" 2 / 12 - getPozoPerforados");
                List<EscaleraProduccionMulti> listTerminados = databaseConnectorClient.getPozosPerforados(
                                idOportunidadObjetivo, version, cuota, declinada, pce, area, fecha, multiFlag);


                // log.info(" 7 / 12 - getProduccionTotalMmbpce");
                ProduccionTotalMmbpce produccionTotalMmbpce = databaseConnectorClient.getProduccionTotalMmbpce(
                                idOportunidadObjetivo, version, cuota, declinada, pce, area, fecha, multiFlag);
                produccionTotalMmbpceArray[indexNumber] = produccionTotalMmbpce.getProduccionTotalMmbpce();

                // log.info(" 9 / 12 - getVectorProduccion");
                List<VectorProduccion> listVectorProduccion = databaseConnectorClient.getVectorProduccion(
                                idOportunidadObjetivo, version, cuota, declinada, pce, area, fecha, multiFlag);
                // log.info(" 10 / 12 - getPrecioHidrocarburo");

                // log.info(" 11 / 12 - getPozosActivos");
                List<PozosActivos> listActivos = databaseConnectorClient.getPozosActivos(idOportunidadObjetivo,
                                version, cuota, declinada, pce, area, fecha, multiFlag);

                // log.info(" 12 / 12 - getFactorInversion");
                FactorInversion factorInversion = databaseConnectorClient
                                .getFactorInversion(idOportunidadObjetivo);

                // log.info(" 13 / 13 - factor calculo");
                FactorCalculo factorCalculo = databaseConnectorClient.getFactorCalculo(idOportunidadObjetivo,
                                version);

                // log.info(" 14 / 14 - getfechaTerminoDate");
                Date fechaTermino = databaseConnectorClient.getfechaTerminoDate(
                                idOportunidadObjetivo, version, cuota, declinada, pce, area, fecha, multiFlag);
                

                SimpleDateFormat formatter = new SimpleDateFormat("dd/MM/yyyy");
                String fechaTerminoStr = formatter.format(fechaTermino);
                String basicAnioInicioString = oportunity.getFechainicio();
                LocalDateTime fechaParam = null;
                if (indexNumber > 0 && fecha != null && !"unexist".equals(fecha)) {
                        if (!lastYearPozosProduccion.isEmpty()) {
                                Object productionValue = lastYearPozosProduccion.values().iterator().next();

                                if (productionValue instanceof Number) {
                                        BigDecimal lastYearProduction = BigDecimal
                                                        .valueOf(((Number) productionValue).doubleValue());

                                        if (!listActivos.isEmpty()) {
                                                PozosActivos firstActivo = listActivos.get(0);

                                                BigDecimal mergedProduction = firstActivo.getPromedioAnual()
                                                                .add(lastYearProduction);

                                                PozosActivos mergedActivo = new PozosActivos(
                                                                firstActivo.getAnio(),
                                                                mergedProduction);

                                                listActivos.set(0, mergedActivo);

                                        }
                                }
                        }
                        try {
                                String decodedFecha = URLDecoder.decode(fecha,
                                                StandardCharsets.UTF_8.toString());
                                DateTimeFormatter[] dateFormatters = {
                                                DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"),
                                                DateTimeFormatter.ofPattern("yyyy-MM-dd"),
                                                DateTimeFormatter.ofPattern("dd/MM/yyyy")
                                };

                                for (DateTimeFormatter dateFormatter : dateFormatters) {
                                        try {
                                                if (dateFormatter.toString().contains("HH:mm:ss")) {
                                                        fechaParam = LocalDateTime.parse(decodedFecha,
                                                                        dateFormatter);
                                                } else {
                                                        fechaParam = LocalDate
                                                                        .parse(decodedFecha, dateFormatter)
                                                                        .atStartOfDay();
                                                }
                                                break;
                                        } catch (Exception ignored) {
                                        }
                                }

                                if (fechaParam != null) {
                                        int year = fechaParam.getYear();

                                        if (fechaParam.getMonthValue() == 12) {
                                                year += 1;
                                        }

                                        basicAnioInicioString = String.valueOf(year);
                                }
                        } catch (Exception e) {
                                log.error("Error parsing date: {}. Using original value.", fecha, e);
                        }
                }

                final String basicAnioInicio = basicAnioInicioString;
                
                assert listTerminados != null;

                Map<Integer, BigDecimal> pozosTerminados = DataProcess.getPozosterminadosByAnio(listTerminados);
                final int currentPozosTerminadosValue = listTerminados.size();

                var anioFinal = Integer.parseInt(basicAnioInicio);
                if (pozosTerminados.size() > 1) {
                        for (Integer key : pozosTerminados.keySet()) {
                                anioFinal = anioFinal > key ? anioFinal : key;
                        }
                }

                Map<String, Double> vectorProduccion = new HashMap<>();
                assert listVectorProduccion != null;
                listVectorProduccion.forEach(item -> vectorProduccion.put(String.valueOf(item.getAanio()),
                                item.getCtotalanual()));
                Map<String, ProduccionDiariaPromedio> currentProduccionDiariaPromedio = DataProcess
                                .calculaProduccionDiariaPromedioByAnio(factorInversion,
                                                vectorProduccion,
                                                oportunity.getIdhidrocarburo());
                Map<String, ProduccionDiariaPromedio> produccionDiariaPromedio = mergeProduccionDiariaPromedio(
                                currentProduccionDiariaPromedio, lastProduccionDiariaPromedio);

                List<EvaluacionEconomica> evaluacionEconomica = new ArrayList<>();
                assert listActivos != null;

                listActivos.forEach(item -> {
                        evaluacionEconomica.add(new EvaluacionEconomica(
                                        item.getAnio(),
                                        null,
                                        produccionDiariaPromedio.get(item.getAnio()),
                                        null,
                                        null,
                                        null,
                                        null));

                });

                return new EvaluacionResponse(null, pce, oportunity, evaluacionEconomica,
                                null, factorCalculo, fechaTerminoStr,
                                currentPozosTerminadosValue, null);


        }
        public Map<String, ProduccionDiariaPromedio> mergeProduccionDiariaPromedio(
                        Map<String, ProduccionDiariaPromedio> newProduccionDiariaPromedio,
                        Map<String, ProduccionDiariaPromedio> lastProduccionDiariaPromedio) {
                if (newProduccionDiariaPromedio == null) {
                        throw new IllegalArgumentException("newProduccionDiariaPromedio cannot be null");
                }

                if (lastProduccionDiariaPromedio == null) {
                        lastProduccionDiariaPromedio = new ConcurrentHashMap<>();
                }
                synchronized (lastProduccionDiariaPromedio) {

                        String lastYearKey = lastProduccionDiariaPromedio.keySet().stream()
                                        .map(Integer::parseInt)
                                        .max(Integer::compare)
                                        .map(String::valueOf)
                                        .orElse(null);

                        if (lastYearKey != null) {
                                ProduccionDiariaPromedio lastYearKeyProduccion = newProduccionDiariaPromedio
                                                .get(lastYearKey);

                                if (lastYearKeyProduccion == null) {
                                        lastYearKeyProduccion = new ProduccionDiariaPromedio();
                                        synchronized (lastProduccionDiariaPromedio) {
                                                lastProduccionDiariaPromedio.put(lastYearKey, lastYearKeyProduccion);
                                        }
                                }

                                ProduccionDiariaPromedio lastYearData = lastProduccionDiariaPromedio.get(lastYearKey);

                                // Handle nulls before performing arithmetic
                                lastYearKeyProduccion.setMbpce(
                                                (lastYearKeyProduccion.getMbpce() != null
                                                                ? lastYearKeyProduccion.getMbpce()
                                                                : 0.0) +
                                                                (lastYearData.getMbpce() != null
                                                                                ? lastYearData.getMbpce()
                                                                                : 0.0));

                                lastYearKeyProduccion.setAceiteTotal(
                                                (lastYearKeyProduccion.getAceiteTotal() != null
                                                                ? lastYearKeyProduccion.getAceiteTotal()
                                                                : 0.0) +
                                                                (lastYearData.getAceiteTotal() != null
                                                                                ? lastYearData.getAceiteTotal()
                                                                                : 0.0));

                                lastYearKeyProduccion.setAceiteExtraPesado(
                                                (lastYearKeyProduccion.getAceiteExtraPesado() != null
                                                                ? lastYearKeyProduccion.getAceiteExtraPesado()
                                                                : 0.0) +
                                                                (lastYearData.getAceiteExtraPesado() != null
                                                                                ? lastYearData.getAceiteExtraPesado()
                                                                                : 0.0));

                                lastYearKeyProduccion.setAceitePesado(
                                                (lastYearKeyProduccion.getAceitePesado() != null
                                                                ? lastYearKeyProduccion.getAceitePesado()
                                                                : 0.0) +
                                                                (lastYearData.getAceitePesado() != null
                                                                                ? lastYearData.getAceitePesado()
                                                                                : 0.0));

                                lastYearKeyProduccion.setAceiteLigero(
                                                (lastYearKeyProduccion.getAceiteLigero() != null
                                                                ? lastYearKeyProduccion.getAceiteLigero()
                                                                : 0.0) +
                                                                (lastYearData.getAceiteLigero() != null
                                                                                ? lastYearData.getAceiteLigero()
                                                                                : 0.0));

                                lastYearKeyProduccion.setAceiteSuperLigero(
                                                (lastYearKeyProduccion.getAceiteSuperLigero() != null
                                                                ? lastYearKeyProduccion.getAceiteSuperLigero()
                                                                : 0.0) +
                                                                (lastYearData.getAceiteSuperLigero() != null
                                                                                ? lastYearData.getAceiteSuperLigero()
                                                                                : 0.0));

                                lastYearKeyProduccion.setGasTotal(
                                                (lastYearKeyProduccion.getGasTotal() != null
                                                                ? lastYearKeyProduccion.getGasTotal()
                                                                : 0.0) +
                                                                (lastYearData.getGasTotal() != null
                                                                                ? lastYearData.getGasTotal()
                                                                                : 0.0));

                                lastYearKeyProduccion.setGasHumedo(
                                                (lastYearKeyProduccion.getGasHumedo() != null
                                                                ? lastYearKeyProduccion.getGasHumedo()
                                                                : 0.0) +
                                                                (lastYearData.getGasHumedo() != null
                                                                                ? lastYearData.getGasHumedo()
                                                                                : 0.0));

                                lastYearKeyProduccion.setGasSeco(
                                                (lastYearKeyProduccion.getGasSeco() != null
                                                                ? lastYearKeyProduccion.getGasSeco()
                                                                : 0.0) +
                                                                (lastYearData.getGasSeco() != null
                                                                                ? lastYearData.getGasSeco()
                                                                                : 0.0));

                                lastYearKeyProduccion.setCondensado(
                                                (lastYearKeyProduccion.getCondensado() != null
                                                                ? lastYearKeyProduccion.getCondensado()
                                                                : 0.0) +
                                                                (lastYearData.getCondensado() != null
                                                                                ? lastYearData.getCondensado()
                                                                                : 0.0));

                        }
                }
                return newProduccionDiariaPromedio;
        }

        public EvaluacionResponse processMultipleEvaluaciones(List<List<Object>> paramList) {
                initializeInstanceVariables();
                initializeArrays(paramList.size());
                double[] produccionTotalMmbpceArray = produccionTotalMmbpceArrayThreadLocal.get();
                List<EvaluacionResponse> responses = new ArrayList<>();
                String currentFecha = "unexist";
                int currentOldPozosTerminadosValue = 0;
                Map<String, Object> lastYearPozosProduccion = new ConcurrentHashMap<>();
                Map<String, ProduccionDiariaPromedio> lastProduccionDiariaPromedio = new ConcurrentHashMap<>();
                lastYearPozosProduccion.clear();
                lastProduccionDiariaPromedio.clear();
                
                FactorCalculoForMulti factorCalculoForMulti = new FactorCalculoForMulti();
                factorCalculoForMulti.setIsMulti(false);
                for (int i = 0; i < paramList.size(); i++) {
                        List<Object> params = paramList.get(i);
                        String fechaToUse = (i == 0) ? "unexist" : currentFecha;
                        int oldPozosTerminadosValueToUse = (i == 0) ? 0 : currentOldPozosTerminadosValue;
                        int idOportunidadObjetivo = getIntValue(params, 0);
                        double pce = getDoubleValue(params, 4);
                        EvaluacionResponse response = getInfoPozosService(
                                        i,
                                        idOportunidadObjetivo, // idOportunidadObjetivo
                                        getIntValue(params, 1), // version
                                        getDoubleValue(params, 2), // cuota
                                        getDoubleValue(params, 3), // declinada
                                        pce, // pce 
                                        getDoubleValue(params, 5), // area
                                        
                                        fechaToUse, // fechaToUse
                                        oldPozosTerminadosValueToUse,
                                        lastYearPozosProduccion, lastProduccionDiariaPromedio);
                        if (i < paramList.size() - 1) {
                                currentFecha = response.getFechaTermino();
                                // both success, calculate lastYearPozosProduccion, last result - 1
                                if (currentFecha != null && !currentFecha.isEmpty()) {

                                        List<EvaluacionEconomica> evaluacionList = new ArrayList<>(
                                                        response.getEvaluacionEconomica());

                                        String[] parts = currentFecha.split("/");
                                        int endYearOfFirstObject = Integer.parseInt(parts[2]);
                                        String endMonthOfFristObject = parts[1];
                                        String opportunityStart = response.getInfoOportunidad()
                                                        .getFechainicio();
                                        int opportunityStartYear = Integer.parseInt(opportunityStart);
                                        int validBetween = 10;
                                        int overLapYear = opportunityStartYear + validBetween;
                                        Boolean freshFlag = false;
                                        Boolean overLapFlag = false;
                                        if (endYearOfFirstObject > overLapYear) {
                                                overLapFlag = true;
                                                int overLapYearStart = overLapYear - 1;
                                                currentFecha = overLapYearStart + "-12-01";
                                        } else if (endMonthOfFristObject.equals("12")) {
                                                freshFlag = true;
                                        }

                                        currentOldPozosTerminadosValue = response.getPozosTerminadosValue();
                                        if (evaluacionList != null
                                                        && !evaluacionList.isEmpty()) {

                                                EvaluacionEconomica lastYear = new EvaluacionEconomica();
                                                if (overLapFlag == false) {
                                                        lastYear = evaluacionList
                                                                        .stream()
                                                                        .max(Comparator.comparingInt(
                                                                                        e -> Integer.parseInt(
                                                                                                        e.getAnio())))
                                                                        .orElseThrow(() -> new IllegalStateException(
                                                                                        "No evaluaciones available"));
                                                } else {
                                                        Optional<EvaluacionEconomica> overLapYearData = evaluacionList
                                                                        .stream()
                                                                        .filter(e -> e.getAnio()
                                                                                        .equals(Integer.toString(
                                                                                                        overLapYear)))
                                                                        .findFirst();

                                                        if (overLapYearData.isPresent()) {
                                                                lastYear = overLapYearData.get();
                                                        } else {
                                                                throw new IllegalStateException(
                                                                                "No data available for the year "
                                                                                                + overLapYear);
                                                        }

                                                }

                                                lastYearPozosProduccion.clear();
                                                if (lastYear.getPozosProduccion() != null) {
                                                        lastYearPozosProduccion.put(lastYear.getAnio(),
                                                                        lastYear.getPozosProduccion()
                                                                                        .getPozosActivos());
                                                }
                                                lastProduccionDiariaPromedio.clear();
                                                if (lastYear.getProduccionDiariaPromedio() != null) {
                                                        lastProduccionDiariaPromedio.put(lastYear.getAnio(),
                                                                        lastYear.getProduccionDiariaPromedio());
                                                }
                                        }

                                        if (pce != 0) {
                                                if (!freshFlag) {
                                                        if (overLapFlag == true) {
                                                                evaluacionList.removeIf(
                                                                                evaluacion -> evaluacion.getAnio()
                                                                                                .equals(Integer.toString(
                                                                                                                overLapYear)));

                                                        } else {
                                                                evaluacionList.remove(evaluacionList.size() - 1);
                                                        }
                                                } else {
                                                        Map<String, Object> updatedPozosProduccion = new HashMap<>();

                                                        for (Map.Entry<String, Object> entry : lastYearPozosProduccion
                                                                        .entrySet()) {
                                                                try {
                                                                        int newKey = Integer.parseInt(
                                                                                        entry.getKey())
                                                                                        + 1;
                                                                        updatedPozosProduccion.put(
                                                                                        String.valueOf(newKey),
                                                                                        0);
                                                                } catch (NumberFormatException e) {
                                                                        updatedPozosProduccion.put(
                                                                                        entry.getKey(),
                                                                                        0);
                                                                }
                                                        }
                                                        lastYearPozosProduccion.clear();
                                                        lastYearPozosProduccion.putAll(updatedPozosProduccion);
                                                        lastProduccionDiariaPromedio.clear();

                                                }
                                        }
                                        response.setEvaluacionEconomica(evaluacionList);
                                }
                        }
                        
                        responses.add(response);
                }
                

                double produccionTotalSum = 0;
                for (int i = 0; i < produccionTotalMmbpceArray.length; i++) {
                        produccionTotalSum += produccionTotalMmbpceArray[i];
                }
                produccionTotalMmbpceArrayThreadLocal.remove();

                ProduccionTotalMmbpce produccionTotalMmbpceParam = new ProduccionTotalMmbpce();
                produccionTotalMmbpceParam.setProduccionTotalMmbpce(produccionTotalSum);
                Map<String, List<EvaluacionEconomica>> evaluacionesByYear = responses.stream()
                                .flatMap(response -> response.getEvaluacionEconomica().stream())
                                .collect(Collectors.groupingBy(EvaluacionEconomica::getAnio));
                List<EvaluacionEconomica> mergedEvaluaciones = evaluacionesByYear.entrySet().stream()
                                .map(entry -> mergeEvaluacionesForYear(entry.getKey(), entry.getValue()))
                                .sorted(Comparator.comparing(EvaluacionEconomica::getAnio))
                                .collect(Collectors.toList());
                
                InformacionOportunidad firstInfoOportunidad = responses.get(0).getInfoOportunidad();
                EvaluacionResponse finalResult = new EvaluacionResponse(
                        0.0,
                        0.0,
                        firstInfoOportunidad,
                        mergedEvaluaciones,
                        null,
                        null);
                return finalResult;
        }

        private double getDoubleValue(List<Object> params, int index) {
                if (params == null || index < 0 || index >= params.size()) {
                        return 0.0;
                }
                Object value = params.get(index);
                return (value instanceof Number) ? ((Number) value).doubleValue() : 0.0;
        }

        private int getIntValue(List<Object> params, int index) {
                if (params == null || index < 0 || index >= params.size()) {
                        return 0;
                }
                Object value = params.get(index);
                return (value instanceof Number) ? ((Number) value).intValue() : 0;
        }

        private EvaluacionEconomica mergeEvaluacionesForYear(String year, List<EvaluacionEconomica> evaluaciones) {
                // Use a new EvaluacionEconomica object initialized to zero
                EvaluacionEconomica merged = new EvaluacionEconomica(year, new Pozo(), new ProduccionDiariaPromedio(),
                        new Ingresos(), new Inversiones(), new Costos(), new FlujoContable());
        
                for (EvaluacionEconomica current : evaluaciones) {
                    merged.setPozosProduccion(null);
                    merged.setProduccionDiariaPromedio(
                        evaluacionMerger.mergeProduccionDiaria(merged.getProduccionDiariaPromedio(), current.getProduccionDiariaPromedio()));
                        merged.setIngresos(null);
                    merged.setInversiones(null);
                    merged.setCostos(null);
                    merged.setFlujoContable(null);
                }
        
                return merged;
        }
        

        public EvaluacionResponse processSimpleEvaluaciones(
                Integer idOportunidadObjetivo,
                Integer version,
                double cuota, double declinada, double pce, double area, double plataformadesarrollo,
                double lineadedescarga, double estacioncompresion,
                double ducto, double bateria, double infra, double perf,
                double term, double infraDES, double perfDES, double termDES, double arbolesSubmarinos,
                double manifolds, double risers, double sistemaDeControl, double cubiertaDeProceso,
                double buquetaqueCompra, double buquetaqueRenta) {


                // log.info(" 1 / 12 - getPozoPerforados");
                InformacionOportunidad oportunity = databaseConnectorClient
                                .getInfoOportunidad(idOportunidadObjetivo);

                if (oportunity == null) {
                        Log.error("InformacionOportunidad is null");
                        throw new RuntimeException("InformacionOportunidad is null");
                }

                // log.info(" 9 / 12 - getVectorProduccion");
                List<VectorProduccion> listVectorProduccion = databaseConnectorClient.getVectorProduccion(
                                idOportunidadObjetivo, version, cuota, declinada, pce, area);

                // log.info(" 12 / 12 - getFactorInversion");
                FactorInversion factorInversion = databaseConnectorClient.getFactorInversion(idOportunidadObjetivo);
                List<PozosActivos> listActivos = databaseConnectorClient.getPozosActivos(idOportunidadObjetivo,
                                version, cuota, declinada, pce, area);
                int anioMin = listActivos.stream()
                        .map(p -> Integer.parseInt(p.getAnio()))
                        .min(Comparator.naturalOrder())
                        .orElse(0);
                if(anioMin >  Integer.parseInt(oportunity.getFechainicioperfexploratorio()) + 1){
                        int inicioVacio = Integer.parseInt(oportunity.getFechainicioperfexploratorio()) + 1;

                        BigDecimal value = new BigDecimal(0);
                        List<PozosActivos> pozosFaltantes = IntStream.rangeClosed(inicioVacio, anioMin - 2)
                                .filter(anio -> listActivos.stream().noneMatch(pozo -> pozo.getAnio().equals(String.valueOf(anio))))
                                .mapToObj( anio -> new PozosActivos(String.valueOf(anio), value))
                                .toList();

                        listActivos.addAll(pozosFaltantes);

                        listActivos.sort(Comparator.comparing(pozo -> Integer.parseInt(pozo.getAnio())));
                }

                Map<String, Double> vectorProduccion = new HashMap<>();
                assert listVectorProduccion != null;
                listVectorProduccion.forEach(item -> vectorProduccion.put(String.valueOf(item.getAanio()),
                                item.getCtotalanual()));

                // log.info("Calculando produccion diaria promedio");
                Map<String, ProduccionDiariaPromedio> produccionDiariaPromedio = DataProcess
                                .calculaProduccionDiariaPromedioByAnio(factorInversion, vectorProduccion,
                                                oportunity.getIdhidrocarburo());

                List<EvaluacionEconomica> evaluacionEconomica = new ArrayList<>();
                assert listActivos != null;
                listActivos.forEach(item -> {
                        evaluacionEconomica.add(new EvaluacionEconomica(item.getAnio(),
                                        null,
                                        produccionDiariaPromedio.get(item.getAnio()),
                                        null,
                                        null, null, null));

                });

                evaluacionEconomica.sort(Comparator.comparing(EvaluacionEconomica::getAnio));

                Double areamasignacion = databaseConnectorClient
                                .getAreakmasignacion(idOportunidadObjetivo, version).getAreakmasignacion();

                return new EvaluacionResponse(areamasignacion, pce, oportunity, evaluacionEconomica,
                                null, null);
        }

}