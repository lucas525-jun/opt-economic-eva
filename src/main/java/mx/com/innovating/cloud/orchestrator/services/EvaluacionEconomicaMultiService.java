package mx.com.innovating.cloud.orchestrator.services;

import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import lombok.extern.slf4j.Slf4j;
import mx.com.innovating.cloud.data.entities.InformacionOportunidad;
import mx.com.innovating.cloud.data.models.CostoOperacion;
import mx.com.innovating.cloud.data.models.FactorInversion;
import mx.com.innovating.cloud.data.models.FactorInversionDesarrollo;
import mx.com.innovating.cloud.data.models.FactorInversionExploratorio;
import mx.com.innovating.cloud.data.models.InformacionInversion;
import mx.com.innovating.cloud.data.models.Paridad;
import mx.com.innovating.cloud.data.models.PozosActivos;
import mx.com.innovating.cloud.data.models.PrecioHidrocarburo;
import mx.com.innovating.cloud.data.models.ProduccionTotalMmbpce;
import mx.com.innovating.cloud.data.models.VectorProduccion;
import mx.com.innovating.cloud.data.models.*;
import mx.com.innovating.cloud.data.repository.DataBaseConnectorRepository;
import mx.com.innovating.cloud.orchestrator.models.*;
import mx.com.innovating.cloud.orchestrator.utilities.DataProcess;
import mx.com.innovating.cloud.data.calculator.FechaInicioService;

import java.io.File;
import java.io.IOException;
import java.math.BigDecimal;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.Year;
import java.time.format.DateTimeFormatter;
import java.util.*;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

@Slf4j
@ApplicationScoped
public class EvaluacionEconomicaMultiService {

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

        private void logObject(String name, Object obj) {
                try {
                        String result = obj != null ? obj.toString() : "null";
                        if (obj != null) {
                                if (obj instanceof Collection) {
                                        result = String.format("Size: %d, Content: %s", ((Collection<?>) obj).size(),
                                                        obj);
                                } else {
                                        result = obj.toString();
                                }
                        }
                        log.info("Result {} - {}: {}", name, result);
                        log.debug("Original map order: {}", obj);

                } catch (Exception e) {
                        log.error("Error logging {} - {}: {}", name, e.getMessage());
                }
        }

        public EvaluacionResponse getInfoPozosService(
                        int indexNumber,
                        Integer idOportunidadObjetivo,
                        Integer version,
                        double cuota, double declinada, double pce, double area, double plataformadesarrollo,
                        double lineadedescarga, double estacioncompresion,
                        double ducto, double bateria, double infra, double perf,
                        double term, double infraDES, double perfDES, double termDES, double arbolesSubmarinos,
                        double manifolds, double risers, double sistemaDeControl, double cubiertaDeProceso,
                        double buquetaqueCompra, double buquetaqueRenta, String fecha, int oldPozosTerminadosValue,
                        Map<String, Object> lastYearPozosProduccion,
                        Map<String, ProduccionDiariaPromedio> lastProduccionDiariaPromedio) {
                double[] produccionTotalMmbpceArray = produccionTotalMmbpceArrayThreadLocal.get();

                if (pce == 0) {
                        // log.info("PCE es 0. Solo se realizarán cálculos de inversión exploratoria.");
                        InformacionOportunidad oportunity = databaseConnectorClient
                                        .getInfoOportunidad(idOportunidadObjetivo);
                        List<EvaluacionEconomica> evaluacionEconomica;

                        // log.info("PCE es 0. Ejecutando cálculos de inversión exploratoria y flujo
                        // contable.");

                        // log.info(" 12 / 12 - getFactorInversion");
                        FactorInversion factorInversion = databaseConnectorClient
                                        .getFactorInversion(idOportunidadObjetivo);
                        factorInversion.setPce(0.0);

                        // log.info(" 7 / 12 - getProduccionTotalMmbpce");
                        ProduccionTotalMmbpce produccionTotalMmbpce = databaseConnectorClient.getProduccionTotalMmbpce(
                                        idOportunidadObjetivo, version, cuota, declinada, pce, area);
                        produccionTotalMmbpceArray[indexNumber] = produccionTotalMmbpce.getProduccionTotalMmbpce();
                        // Lógica de preparación de evaluación económica dentro del `if`
                        FactorInversionExploratorio fiExploratoria = new FactorInversionExploratorio();

                        fiExploratoria.setInfraestructura(infra);
                        fiExploratoria.setPerforacion(perf);
                        fiExploratoria.setTerminacion(term);
                        if (indexNumber > 0) {

                                fiExploratoria.setInfraestructura(0.0);
                                fiExploratoria.setPerforacion(0.0);
                                fiExploratoria.setTerminacion(0.0);

                        }
                        // log.info("fiExploratoria" + fiExploratoria.getInfraestructura());

                        Paridad paridad = databaseConnectorClient.getParidad(
                                        Integer.valueOf(oportunity.getFechainicioperfexploratorio()));
                        var invExploratoria = DataProcess.calculaInversionExploratoria(fiExploratoria,
                                        paridad.getParidad());

                        evaluacionEconomica = new ArrayList<>();
                        var inversionesExpAnioInicioPerf = new Inversiones(
                                        null, invExploratoria.getExploratoria(), invExploratoria.getPerforacionExp(),
                                        invExploratoria.getTerminacionExp(), invExploratoria.getInfraestructuraExp(),
                                        0.0, 0.0,
                                        0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0,
                                        0.0, 0.0);

                        evaluacionEconomica.add(
                                        new EvaluacionEconomica(
                                                        oportunity.getFechainicioperfexploratorio(),
                                                        null, null, null, inversionesExpAnioInicioPerf, null, null));

                        // **Agregar flujo contable**
                        DataProcess.finalProcessInversiones(evaluacionEconomica);
                        DataProcess.calculaFlujoContable(evaluacionEconomica);

                        var flujosContablesTotales = DataProcess.calculaFlujosContablesTotales(
                                        evaluacionEconomica, produccionTotalMmbpce, factorInversion, pce);

                        Double areamasignacion = databaseConnectorClient
                                        .getAreakmasignacion(idOportunidadObjetivo, version).getAreakmasignacion();

                        return new EvaluacionResponse(areamasignacion, pce, oportunity, evaluacionEconomica,
                                        flujosContablesTotales, null, null, 0, null);

                } else {
                        // log.info("PCE distinto de 0. Ejecutando flujo completo.");

                        // log.info(" 1 / 12 - getPozoPerforados");
                        InformacionOportunidad oportunity = databaseConnectorClient
                                        .getInfoOportunidad(idOportunidadObjetivo);

                        // log.info(" 2 / 12 - getPozoPerforados");
                        List<EscaleraProduccionMulti> listTerminados = databaseConnectorClient.getPozosPerforados(
                                        idOportunidadObjetivo, version, cuota, declinada, pce, area, fecha);

                        // log.info(" 3 / 12 - getFactorInversionExploratorio");
                        FactorInversionExploratorio fiExploratoria = new FactorInversionExploratorio();
                        fiExploratoria.setInfraestructura(infra);
                        fiExploratoria.setPerforacion(perf);
                        fiExploratoria.setTerminacion(term);
                        if (indexNumber > 0) { // from the second object, don't need to calculate exploration.
                                fiExploratoria.setInfraestructura(0.0);
                                fiExploratoria.setPerforacion(0.0);
                                fiExploratoria.setTerminacion(0.0);
                        }
                        // log.info("-------------------------------------------------");
                        // log.info("fiExploratoria" + fiExploratoria.getInfraestructura());

                        // log.info(" 4 / 12 - getFactorInversionDesarrollo");

                        FactorInversionDesarrollo fiDesarrollo = new FactorInversionDesarrollo(); // enviar de micro
                        fiDesarrollo.setInfraestructura(infraDES);
                        fiDesarrollo.setPerforacion(perfDES);
                        fiDesarrollo.setTerminacion(termDES);

                        // log.info(" 5 / 12 - getInformacionInversion");
                        InformacionInversion infoInversion = new InformacionInversion();
                        infoInversion.setLineadedescarga(lineadedescarga);
                        infoInversion.setArbolessubmarinos(arbolesSubmarinos);
                        infoInversion.setSistemasdecontrol(sistemaDeControl);

                        // Initialize all investment parameters to zero

                        infoInversion.setDucto(ducto);
                        infoInversion.setBateria(bateria);
                        infoInversion.setPlataformadesarrollo(plataformadesarrollo);
                        infoInversion.setEstacioncompresion(estacioncompresion);
                        infoInversion.setRisers(risers);
                        infoInversion.setManifolds(manifolds);
                        infoInversion.setCubiertadeproces(cubiertaDeProceso);
                        if (indexNumber > 0) {
                                if (fecha != null && !"unexist".equals(fecha)) {
                                        infoInversion.setDucto(0.0);
                                        infoInversion.setBateria(0.0);
                                        infoInversion.setPlataformadesarrollo(0.0);
                                        infoInversion.setEstacioncompresion(0.0);
                                        infoInversion.setRisers(0.0);
                                        infoInversion.setManifolds(0.0);
                                        infoInversion.setCubiertadeproces(0.0);
                                }
                        }

                        infoInversion.setBuquetanquecompra(buquetaqueCompra);
                        infoInversion.setBuquetanquerenta(buquetaqueRenta);

                        // log.info(" 6 / 12 - getCostoOperacion");
                        List<CostoOperacion> listCostoOperacion = databaseConnectorClient
                                        .getCostoOperacion(oportunity.getIdproyecto());

                        // log.info(" 7 / 12 - getProduccionTotalMmbpce");
                        ProduccionTotalMmbpce produccionTotalMmbpce = databaseConnectorClient.getProduccionTotalMmbpce(
                                        idOportunidadObjetivo, version, cuota, declinada, pce, area, fecha);
                        produccionTotalMmbpceArray[indexNumber] = produccionTotalMmbpce.getProduccionTotalMmbpce();

                        // log.info(" 8 / 12 - getParidad");
                        Paridad paridad = databaseConnectorClient.getParidad(
                                        Integer.valueOf(oportunity.getFechainicioperfexploratorio()));
                        // log.info(" 9 / 12 - getVectorProduccion");
                        List<VectorProduccion> listVectorProduccion = databaseConnectorClient.getVectorProduccion(
                                        idOportunidadObjetivo, version, cuota, declinada, pce, area, fecha);

                        // log.info(" 10 / 12 - getPrecioHidrocarburo");
                        List<PrecioHidrocarburo> listPrecios = databaseConnectorClient
                                        .getPrecioHidrocarburo(idOportunidadObjetivo, oportunity.getIdprograma());

                        // log.info(" 11 / 12 - getPozosActivos");
                        List<PozosActivos> listActivos = databaseConnectorClient.getPozosActivos(idOportunidadObjetivo,
                                        version, cuota, declinada, pce, area, fecha);

                        // log.info(" 12 / 12 - getFactorInversion");
                        FactorInversion factorInversion = databaseConnectorClient
                                        .getFactorInversion(idOportunidadObjetivo);

                        // log.info(" 13 / 13 - factor calculo");
                        FactorCalculo factorCalculo = databaseConnectorClient.getFactorCalculo(idOportunidadObjetivo,
                                        version);

                        // log.info(" 14 / 14 - getfechaTerminoDate");
                        Date fechaTermino = databaseConnectorClient.getfechaTerminoDate(
                                        idOportunidadObjetivo, version, cuota, declinada, pce, area, fecha);
                        

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
                        String previousYear = indexNumber == 0 ? oportunity.getFechainicioperfexploratorio()
                                        : String.valueOf(Integer.parseInt(basicAnioInicio) - 1);

                        // log.info("Obteniendo la informacion de los pozos perforados");
                        assert listTerminados != null;
                        Map<Integer, BigDecimal> pozosPerforados = DataProcess.getPozosPerforadosByAnio(listTerminados,
                                        basicAnioInicio);

                        // log.info("Obteniendo la informacion de los pozos terminados");
                        Map<Integer, BigDecimal> pozosTerminados = DataProcess.getPozosterminadosByAnio(listTerminados);
                        final int currentPozosTerminadosValue = listTerminados.size();
                        final Map<Integer, BigDecimal> adjustedPozosPerforados;

                        // final Map<Integer, BigDecimal> adjustedPozosPerforados = pozosPerforados;
                        if (oldPozosTerminadosValue == 0) {
                                adjustedPozosPerforados = pozosPerforados != null ? new HashMap<>(pozosPerforados)
                                                : new HashMap<>();
                        } else {
                                adjustedPozosPerforados = adjustPozosPerforados(
                                                pozosPerforados,
                                                oldPozosTerminadosValue);
                        }

                        List<OportunidadPlanDesarrollo> planDesarrollo = databaseConnectorClient
                                        .getPlanDesarrolloByOportunidad(idOportunidadObjetivo, version);

                        var anioFinal = Integer.parseInt(basicAnioInicio);
                        if (pozosTerminados.size() > 1) {
                                for (Integer key : pozosTerminados.keySet()) {
                                        anioFinal = anioFinal > key ? anioFinal : key;
                                }
                        }

                        int finalAnioFinal = anioFinal;

                        // log.info("Obteniendo vector de produccion");
                        Map<String, Double> vectorProduccion = new HashMap<>();
                        assert listVectorProduccion != null;
                        listVectorProduccion.forEach(item -> vectorProduccion.put(String.valueOf(item.getAanio()),
                                        item.getCtotalanual()));
                        // log.info("Calculando produccion diaria promedio");
                        Map<String, ProduccionDiariaPromedio> currentProduccionDiariaPromedio = DataProcess
                                        .calculaProduccionDiariaPromedioByAnio(factorInversion,
                                                        vectorProduccion,
                                                        oportunity.getIdhidrocarburo());
                        Map<String, ProduccionDiariaPromedio> produccionDiariaPromedio = mergeProduccionDiariaPromedio(
                                        currentProduccionDiariaPromedio, lastProduccionDiariaPromedio);
                        Map<String, Double> preciosMap = new HashMap<>();
                        assert listPrecios != null;
                        listPrecios.forEach(
                                        item -> preciosMap.put(item.getAnioprecio() + "-" + item.getIdhidrocarburo(),
                                                        item.getPrecio()));

                        // log.info("Calculando Ingresos");
                        assert paridad != null;
                        Map<String, Ingresos> ingresosMap = DataProcess.calculaIngresosByAnio(paridad,
                                        produccionDiariaPromedio, preciosMap);

                        Map<String, Double> costoOperacionMap = new HashMap<>();
                        assert listCostoOperacion != null;
                        listCostoOperacion.forEach(element -> costoOperacionMap
                                        .put(element.getAnio() + "-" + element.getIdcostooperacion(),
                                                        element.getGasto()));

                        Map<String, Costos> costosMap = DataProcess.calculaCostosByAnio(costoOperacionMap,
                                        produccionDiariaPromedio, paridad);

                        // log.info("Generando Respuesta");
                        List<EvaluacionEconomica> evaluacionEconomica = new ArrayList<>();
                        assert listActivos != null;
                        
                        CalculoNumPozosResult calculoPozosTotales = databaseConnectorClient.getPozosTotales(version, idOportunidadObjetivo, cuota, declinada, pce, area);
                        
                        double pozosTotales = calculoPozosTotales.getNPozos();
                        // System.err.println("from : indexNumber : " + indexNumber + ", idOportunidadObjetivo : " + idOportunidadObjetivo + ", pce : " + pce + ", pozosTotales : " + pozosTotales);
                       
                        double cantManifolds = Math.ceil(pozosTotales / 6.0);
                        


                        listActivos.forEach(item -> {
                                var anioActualInteger = Integer.parseInt(item.getAnio());
                                int yearDays = Year.of(anioActualInteger).length();
                                BigDecimal perforado = new BigDecimal(0);
                                BigDecimal terminado = new BigDecimal(0);

                                Integer aniosPerforacion = adjustedPozosPerforados.size();
                                // if (fecha != null && !"unexist".equals(fecha)) {
                                aniosPerforacion = 2;
                                // }
                                var inversionesAnioActual = new Inversiones();
                                if (pozosTerminados.containsKey(anioActualInteger)) {
                                        terminado = pozosTerminados.get(anioActualInteger);
                                }

                                if (adjustedPozosPerforados.containsKey(anioActualInteger)
                                                && item.getAnio().equals(basicAnioInicio)) {

                                        // 1. calculate investment for the exploration.
                                        // for the 1st object, real drill-well number is -1 because 1 well is drilled in
                                        // exploration
                                        // if (fecha != null && !"unexist".equals(fecha)) {
                                        // perforado = adjustedPozosPerforados.get(anioActualInteger)
                                        // .subtract(new BigDecimal(1))
                                        // .max(BigDecimal.ZERO);
                                        // } else {

                                        // }
                                        if (fecha == null || "unexist".equals(fecha)) {
                                                perforado = adjustedPozosPerforados.get(anioActualInteger)
                                                                .subtract(new BigDecimal(1))
                                                                .max(BigDecimal.ZERO);
                                        } else {
                                                perforado = adjustedPozosPerforados.get(anioActualInteger);

                                        }
                                        assert fiExploratoria != null;
                                        var invExploratoria = DataProcess.calculaInversionExploratoria(fiExploratoria,
                                                        paridad.getParidad());
                                        var inversionesExpAnioInicioPerf = new Inversiones(
                                                        null, invExploratoria.getExploratoria(),
                                                        invExploratoria.getPerforacionExp(),
                                                        invExploratoria.getTerminacionExp(),
                                                        invExploratoria.getInfraestructuraExp(), null, null, null,
                                                        null, null, null, null,
                                                        null, null, null,
                                                        null, null, null, null, null,
                                                        null, null, null, null, null);
                                        evaluacionEconomica.add(
                                                        new EvaluacionEconomica(
                                                                        previousYear,
                                                                        null, null, null,
                                                                        inversionesExpAnioInicioPerf, null, null));
                                        // 2. real drill
                                        assert terminado != null;
                                        assert fiDesarrollo != null;

                                        var invDesarrollo = DataProcess.calculaInversionDesarrollo(fiDesarrollo,
                                                        paridad.getParidad(), terminado, perforado, aniosPerforacion);

                                        var anioCompare = Integer.parseInt(item.getAnio()) - 1;
                                        AtomicBoolean existe = new AtomicBoolean(false);

                                        evaluacionEconomica.forEach(evaluacion -> {
                                                if (evaluacion.getAnio().equals(Integer.toString(anioCompare))) {
                                                        existe.set(true);

                                                        evaluacion.getInversiones()
                                                                        .setDesarrolloSinOperacional(invDesarrollo
                                                                                        .getDesarrolloSinOperacional());
                                                        evaluacion.getInversiones().setTerminacionDes(
                                                                        invDesarrollo.getTerminacionDes());
                                                        evaluacion.getInversiones().setPerforacionDes(
                                                                        invDesarrollo.getPerforacionDes());
                                                        evaluacion.getInversiones()
                                                                        .setInfraestructuraDes(invDesarrollo
                                                                                        .getInfraestructuraDes());
                                                        evaluacion.getInversiones()
                                                                        .setDesarrollo(invDesarrollo.getDesarrollo());

                                                }
                                        });

                                        

                                        assert infoInversion != null;
                                        var inversionesAnioAnterior = new Inversiones();

                                        if (!existe.get()) {

                                                inversionesAnioAnterior.setDesarrolloSinOperacional(
                                                                invDesarrollo.getDesarrolloSinOperacional());
                                                inversionesAnioAnterior
                                                                .setPerforacionDes(invDesarrollo.getPerforacionDes());
                                                inversionesAnioAnterior
                                                                .setTerminacionDes(invDesarrollo.getTerminacionDes());
                                                inversionesAnioAnterior.setInfraestructuraDes(
                                                                invDesarrollo.getInfraestructuraDes());
                                                inversionesAnioAnterior.setDesarrollo(
                                                                invDesarrollo.getDesarrolloSinOperacional());

                                                
                                                evaluacionEconomica.add(new EvaluacionEconomica(
                                                                        Integer.toString(anioCompare),
                                                                        null, null, null, inversionesAnioAnterior, null, null));
                                                        
                                        }
                                        
                                        
                                        double ductos = 0.0;
                                        double plataformasDesarrollo = 0.0;
                                        double duracionMax = planDesarrollo.stream().mapToDouble(OportunidadPlanDesarrollo::getDuracion).max().orElseThrow(() -> new NoSuchElementException("La lista está vacía"));

                                        for (OportunidadPlanDesarrollo plan : planDesarrollo) {

                                                // String nombreVersion = plan.getNombreVersion();
                                                // char lastChar = nombreVersion.charAt(nombreVersion.length() - 1); // Obtiene
                                                //                                                                   // el
                                                                                                                  // último
                                                                                                                  // carácter
                                                // if (Character.isDigit(lastChar)) { // Verifica si el último carácter es
                                                //                                    // un dígito

                                                        // int lastDigit = Character.getNumericValue(lastChar); // Convierte
                                                        // el
                                                        // carácter
                                                        // a número
                                                if (duracionMax >= 2) { 
                                                        var anioInicioPerfexploratorio = Integer.parseInt(
                                                                        oportunity.getFechainicioperfexploratorio());
                                                        var anioInicio = Integer
                                                                        .parseInt(basicAnioInicio);
                                                        if (anioInicioPerfexploratorio
                                                                        + plan.getDuracion() == anioInicio) {

                                                                
                                                                double ductosForInit = infoInversion.getDucto()
                                                                                * paridad.getParidad();

                                                                double plataformasDesarrolloForInit = infoInversion
                                                                                .getPlataformadesarrollo()
                                                                                * paridad.getParidad();
                                                                inversionesAnioAnterior.setDuctos(ductosForInit);
                                                                inversionesAnioAnterior.setPlataformaDesarrollo(
                                                                        plataformasDesarrolloForInit);

                                                                var sistemaDeControlG = infoInversion
                                                                                .getSistemasdecontrol()
                                                                                * paridad.getParidad()
                                                                                * cantManifolds;
                                                                inversionesAnioAnterior.setSistemaDeControl(
                                                                                sistemaDeControlG);
                                                                var cubiertaProcesosG = infoInversion
                                                                                .getCubiertadeproces()
                                                                                * paridad.getParidad()
                                                                                * cantManifolds;
                                                                inversionesAnioAnterior.setCubiertaProcesos(
                                                                        cubiertaProcesosG);
                                                                double risersG = infoInversion.getRisers()
                                                                * paridad.getParidad()
                                                                * cantManifolds;
                                                                
                                                                inversionesAnioAnterior.setRisers(risersG);

                                                                // if (!existe.get()) {

                                                                //         inversionesAnioAnterior.setDesarrolloSinOperacional(
                                                                //                         invDesarrollo.getDesarrolloSinOperacional());
                                                                //         inversionesAnioAnterior
                                                                //                         .setPerforacionDes(invDesarrollo.getPerforacionDes());
                                                                //         inversionesAnioAnterior
                                                                //                         .setTerminacionDes(invDesarrollo.getTerminacionDes());
                                                                //         inversionesAnioAnterior.setInfraestructuraDes(
                                                                //                         invDesarrollo.getInfraestructuraDes());
                                                                //         inversionesAnioAnterior.setDesarrollo(
                                                                //                         invDesarrollo.getDesarrolloSinOperacional());

                                                                        
                                                                //         evaluacionEconomica.add(new EvaluacionEconomica(
                                                                //                                 Integer.toString(anioCompare),
                                                                //                                 null, null, null, inversionesAnioAnterior, null, null));
                                                                                
                                                                // } else {
                                                                        // evaluacionEconomica.forEach(evaluacion -> {
                                                                        //         if (evaluacion.getAnio().equals(Integer.toString(anioCompare))) {
                                                                        //                 evaluacion.getInversiones().setDuctos(ductosForInit);
                                                                        //                 evaluacion.getInversiones().setPlataformaDesarrollo(plataformasDesarrolloForInit);
                                                                        //                 evaluacion.getInversiones().setSistemaDeControl(sistemaDeControlG);
                                                                        //                 evaluacion.getInversiones().setCubiertaProcesos(cubiertaProcesosG);
                                                                        //                 evaluacion.getInversiones().setRisers(risersG);
                                                                        //         }
                                                                        // });
                                                                
                                                                // }
                                                                        
                                                        } else {
                                                                var arbolesSubmarinosG = infoInversion
                                                                                .getArbolessubmarinos()
                                                                                * paridad.getParidad()
                                                                                * pozosTotales;
                                                                inversionesAnioActual.setArbolSubmarinos(
                                                                                arbolesSubmarinosG);
                                                                var manifoldsG = infoInversion.getManifolds()
                                                                                * paridad.getParidad()
                                                                                * cantManifolds;
                                                                inversionesAnioActual.setManifolds(manifoldsG);
                                                                var estacionCompresionG = infoInversion
                                                                                .getEstacioncompresion()
                                                                                * paridad.getParidad();
                                                                inversionesAnioActual.setEstacionCompresion(
                                                                                estacionCompresionG);
                                                                var bateriaG = infoInversion.getBateria()
                                                                                * paridad.getParidad();
                                                                inversionesAnioActual.setBateria(bateriaG);
                                                                var buqueTanqueCompraG = infoInversion
                                                                                .getBuquetanquecompra()
                                                                                * paridad.getParidad();
                                                                inversionesAnioActual.setBuqueTanqueCompra(
                                                                                buqueTanqueCompraG);
                                                                var buqueTanqueRentaG = infoInversion
                                                                                .getBuquetanquerenta()
                                                                                * paridad.getParidad();
                                                                inversionesAnioActual.setBuqueTanqueRenta(
                                                                                buqueTanqueRentaG);
                                                        }
                                                } else {
                                                        ductos = infoInversion.getDucto()
                                                                        * paridad.getParidad();

                                                        plataformasDesarrollo = infoInversion
                                                                        .getPlataformadesarrollo()
                                                                        * paridad.getParidad();
                                                        inversionesAnioActual.setDuctos(ductos);
                                                        inversionesAnioActual.setPlataformaDesarrollo(
                                                                        plataformasDesarrollo);
                                                }
                                        }
                                        // logObject("inversionesAnioAnterior : ", inversionesAnioAnterior);
                                        // logObject("evaluacionEconomica : ", evaluacionEconomica);

                                        var lineaDescarga = infoInversion.getLineadedescarga() * terminado.doubleValue()
                                                        * paridad.getParidad();

                                        double futuroDesarrollo = 0.0;
                                        double mantenimientoPozos = 0.0;
                                        double mantenimientoInfraestructuraFutDes = 0.0;

                                        if (costoOperacionMap.containsKey(item.getAnio() + "-19")) {
                                                futuroDesarrollo = costoOperacionMap.get(item.getAnio() + "-19")
                                                                * produccionDiariaPromedio.get(item.getAnio())
                                                                                .getMbpce()
                                                                * yearDays
                                                                * paridad.getParidad() / 1000;
                                        } else {
                                                log.error("No existen datos para el año {}: {}", "costo",
                                                                item.getAnio());
                                        }
                                        if (costoOperacionMap.containsKey(item.getAnio() + "-25")) {
                                                mantenimientoPozos = costoOperacionMap.get(item.getAnio() + "-25")
                                                                * produccionDiariaPromedio.get(item.getAnio())
                                                                                .getMbpce()
                                                                * yearDays
                                                                * paridad.getParidad() / 1000;
                                        } else {
                                                log.error("No existen datos para el año {}: {}", "costo",
                                                                item.getAnio());
                                        }
                                        if (costoOperacionMap.containsKey(item.getAnio() + "-23")) {
                                                mantenimientoInfraestructuraFutDes = costoOperacionMap
                                                                .get(item.getAnio() + "-23")
                                                                * produccionDiariaPromedio.get(item.getAnio())
                                                                                .getMbpce()
                                                                * yearDays
                                                                * paridad.getParidad() / 1000;
                                                // System.err.println("mantenimientoInfraestructuraFutDes"
                                                // + mantenimientoInfraestructuraFutDes);
                                                // System.err.println("fecha : " + fecha);
                                                // System.err.println("year : " + basicAnioInicio);
                                        } else {
                                                log.error("No existen datos para el año {}: {}", "costo",
                                                                item.getAnio());
                                        }
                                        inversionesAnioActual.setLineaDescarga(lineaDescarga);
                                        inversionesAnioActual.setOperacionalFuturoDesarrollo(futuroDesarrollo);
                                        inversionesAnioActual.setMantenimientoDePozos(mantenimientoPozos);
                                        inversionesAnioActual.setMantenimientoInfraestructuraFuturoDesarrollo(
                                                        mantenimientoInfraestructuraFutDes);
                                } else if (adjustedPozosPerforados.containsKey(anioActualInteger)) {
                                        perforado = adjustedPozosPerforados.get(anioActualInteger);

                                        assert terminado != null;
                                        var terminadoFinal = terminado;
                                        if (anioActualInteger == finalAnioFinal && (fecha == null
                                                        || "unexist".equals(fecha))) {
                                                terminadoFinal = terminado.subtract(new BigDecimal(1));
                                        }

                                        assert fiDesarrollo != null;
                                        var invDesarrollo = DataProcess.calculaInversionDesarrollo(fiDesarrollo,
                                                        paridad.getParidad(), terminadoFinal, perforado,
                                                        aniosPerforacion);
                                        var anioInversion = Integer.parseInt(item.getAnio()) - 1;
                                        evaluacionEconomica.forEach(element -> {
                                                if (element.getAnio().equals(Integer.toString(anioInversion))) {
                                                        element.getInversiones()
                                                                        .setDesarrolloSinOperacional(invDesarrollo
                                                                                        .getDesarrolloSinOperacional());
                                                        element.getInversiones().setTerminacionDes(
                                                                        invDesarrollo.getTerminacionDes());
                                                        element.getInversiones().setPerforacionDes(
                                                                        invDesarrollo.getPerforacionDes());
                                                        element.getInversiones()
                                                                        .setInfraestructuraDes(invDesarrollo
                                                                                        .getInfraestructuraDes());
                                                }
                                        });

                                        assert infoInversion != null;
                                        var lineaDescarga = infoInversion.getLineadedescarga() * terminado.doubleValue()
                                                        * paridad.getParidad();
                                        double futuroDesarrollo = 0.0;
                                        double mantenimientoPozos = 0.0;
                                        double mantenimientoInfraestructuraFutDes = 0.0;
                                        if (costoOperacionMap.containsKey(item.getAnio() + "-19")) {
                                                futuroDesarrollo = costoOperacionMap.get(item.getAnio() + "-19")
                                                                * produccionDiariaPromedio.get(item.getAnio())
                                                                                .getMbpce()
                                                                * yearDays
                                                                * paridad.getParidad() / 1000;
                                        } else {
                                                log.error("No existen datos para el año {}: {}", "costo",
                                                                item.getAnio());
                                        }
                                        if (costoOperacionMap.containsKey(item.getAnio() + "-25")) {

                                                mantenimientoPozos = costoOperacionMap.get(item.getAnio() + "-25")
                                                                * produccionDiariaPromedio.get(item.getAnio())
                                                                                .getMbpce()
                                                                * yearDays
                                                                * paridad.getParidad() / 1000;
                                        } else {
                                                log.error("No existen datos para el año {}: {}", "costo",
                                                                item.getAnio());
                                        }
                                        if (costoOperacionMap.containsKey(item.getAnio() + "-23")) {

                                                mantenimientoInfraestructuraFutDes = costoOperacionMap
                                                                .get(item.getAnio() + "-23")
                                                                * produccionDiariaPromedio.get(item.getAnio())
                                                                                .getMbpce()
                                                                * yearDays
                                                                * paridad.getParidad() / 1000;
                                        } else {
                                                log.error("No existen datos para el año {}: {}", "costo",
                                                                item.getAnio());
                                        }

                                        inversionesAnioActual.setLineaDescarga(lineaDescarga);
                                        inversionesAnioActual.setOperacionalFuturoDesarrollo(futuroDesarrollo);
                                        inversionesAnioActual.setMantenimientoDePozos(mantenimientoPozos);
                                        inversionesAnioActual.setMantenimientoInfraestructuraFuturoDesarrollo(
                                                        mantenimientoInfraestructuraFutDes);
                                                        // logObject("2 : ", inversionesAnioActual);
                                } else if (pozosTerminados.containsKey(anioActualInteger)) {

                                        terminado = pozosTerminados.get(anioActualInteger);

                                        var terminadoFinal = terminado;

                                        if (anioActualInteger == finalAnioFinal) {
                                                if (fecha == null || "unexist".equals(fecha)) {
                                                        terminadoFinal = terminado.subtract(new BigDecimal(1));
                                                }
                                        }

                                        assert fiDesarrollo != null;
                                        var invDesarrollo = DataProcess.calculaInversionDesarrollo(fiDesarrollo,
                                                        paridad.getParidad(), terminadoFinal, perforado,
                                                        aniosPerforacion);
                                        var anioInversion = Integer.parseInt(item.getAnio()) - 1;
                                        evaluacionEconomica.forEach(element -> {
                                                if (element.getAnio().equals(Integer.toString(anioInversion))) {
                                                        element.getInversiones()
                                                                        .setDesarrolloSinOperacional(invDesarrollo
                                                                                        .getDesarrolloSinOperacional());
                                                        element.getInversiones().setTerminacionDes(
                                                                        invDesarrollo.getTerminacionDes());
                                                        element.getInversiones().setPerforacionDes(
                                                                        invDesarrollo.getPerforacionDes());
                                                        element.getInversiones()
                                                                        .setInfraestructuraDes(invDesarrollo
                                                                                        .getInfraestructuraDes());
                                                }
                                        });

                                        assert infoInversion != null;
                                        var lineaDescarga = infoInversion.getLineadedescarga() * terminado.doubleValue()
                                                        * paridad.getParidad();

                                        double futuroDesarrollo = 0.0;
                                        double mantenimientoPozos = 0.0;
                                        double mantenimientoInfraestructuraFutDes = 0.0;
                                        if (costoOperacionMap.containsKey(item.getAnio() + "-19")) {
                                                futuroDesarrollo = costoOperacionMap.get(item.getAnio() + "-19")
                                                                * produccionDiariaPromedio.get(item.getAnio())
                                                                                .getMbpce()
                                                                * yearDays
                                                                * paridad.getParidad() / 1000;
                                        } else {
                                                log.error("No existen datos para el año {}: {}", "costo",
                                                                item.getAnio());
                                        }

                                        if (costoOperacionMap.containsKey(item.getAnio() + "-25")) {
                                                mantenimientoPozos = costoOperacionMap.get(item.getAnio() + "-25")
                                                                * produccionDiariaPromedio.get(item.getAnio())
                                                                                .getMbpce()
                                                                * yearDays
                                                                * paridad.getParidad() / 1000;
                                        } else {
                                                log.error("No existen datos para el año {}: {}", "costo",
                                                                item.getAnio());
                                        }
                                        if (costoOperacionMap.containsKey(item.getAnio() + "-23")) {
                                                mantenimientoInfraestructuraFutDes = costoOperacionMap
                                                                .get(item.getAnio() + "-23")
                                                                * produccionDiariaPromedio.get(item.getAnio())
                                                                                .getMbpce()
                                                                * yearDays
                                                                * paridad.getParidad() / 1000;
                                        } else {
                                                log.error("No existen datos para el año {}: {}", "costo",
                                                                item.getAnio());
                                        }

                                        inversionesAnioActual.setLineaDescarga(lineaDescarga);
                                        inversionesAnioActual.setOperacionalFuturoDesarrollo(futuroDesarrollo);
                                        inversionesAnioActual.setMantenimientoDePozos(mantenimientoPozos);
                                        inversionesAnioActual.setMantenimientoInfraestructuraFuturoDesarrollo(
                                                        mantenimientoInfraestructuraFutDes);
                                                        // logObject("3 : ", inversionesAnioActual);

                                }

                                evaluacionEconomica.add(new EvaluacionEconomica(
                                                item.getAnio(),
                                                new Pozo(item.getPromedioAnual(),
                                                                perforado,
                                                                terminado),
                                                produccionDiariaPromedio.get(item.getAnio()),
                                                ingresosMap.get(item.getAnio()),
                                                inversionesAnioActual,
                                                costosMap.get(item.getAnio()),
                                                null));

                        });

                        DataProcess.finalProcessInversiones(evaluacionEconomica);

                        DataProcess.calculaFlujoContable(evaluacionEconomica);

                        var flujosContablesTotales = DataProcess.calculaFlujosContablesTotales(evaluacionEconomica,
                                        produccionTotalMmbpce, factorInversion, pce);

                        Double areamasignacion = databaseConnectorClient
                                        .getAreakmasignacion(idOportunidadObjetivo, version).getAreakmasignacion();

                        return new EvaluacionResponse(areamasignacion, pce, oportunity, evaluacionEconomica,
                                        flujosContablesTotales, factorCalculo, fechaTerminoStr,
                                        currentPozosTerminadosValue, null);

                }

        }

        public Map<Integer, BigDecimal> adjustPozosPerforados(
                        Map<Integer, BigDecimal> pozosPerforados,
                        int oldPozosTerminadosValue) {

                if (pozosPerforados.isEmpty()) {
                        return new HashMap<>();
                }

                // Get the first year (minimum year)
                int firstYear = pozosPerforados.keySet().stream()
                                .min(Integer::compareTo)
                                .orElseThrow(() -> new IllegalArgumentException("Map cannot be empty"));

                // Get the second year if it exists (next year after first year)
                Optional<Integer> secondYear = pozosPerforados.keySet().stream()
                                .filter(year -> year > firstYear)
                                .min(Integer::compareTo);

                BigDecimal firstYearValue = pozosPerforados.get(firstYear);
                BigDecimal oldValue = BigDecimal.valueOf(oldPozosTerminadosValue);

                Map<Integer, BigDecimal> result = new HashMap<>();

                if (firstYearValue.compareTo(oldValue) < 0) {
                        // If first year value is less than old value
                        BigDecimal difference = oldValue.subtract(firstYearValue);
                        result.put(firstYear, BigDecimal.ZERO);

                        // Only adjust second year if it exists
                        if (secondYear.isPresent()) {
                                BigDecimal secondYearValue = pozosPerforados.get(secondYear.get());
                                BigDecimal adjustedSecondYear = secondYearValue.subtract(difference);
                                result.put(secondYear.get(), adjustedSecondYear.compareTo(BigDecimal.ZERO) < 0
                                                ? BigDecimal.ZERO
                                                : adjustedSecondYear);
                        }
                } else if (firstYearValue.compareTo(oldValue) > 0) {
                        // If first year value is greater than old value
                        result.put(firstYear, firstYearValue.subtract(oldValue));

                        // Keep second year as is if it exists
                        secondYear.ifPresent(year -> result.put(year, pozosPerforados.get(year)));
                } else {
                        // If first year value equals old value
                        result.put(firstYear, BigDecimal.ZERO);

                        // Keep second year as is if it exists
                        secondYear.ifPresent(year -> result.put(year, pozosPerforados.get(year)));
                }

                return result;
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
                double sumPCE = 0.0;
                List<EvaluacionResponse> responses = new ArrayList<>();
                String currentFecha = "unexist";
                int currentOldPozosTerminadosValue = 0;
                Map<String, Object> lastYearPozosProduccion = new ConcurrentHashMap<>();
                Map<String, ProduccionDiariaPromedio> lastProduccionDiariaPromedio = new ConcurrentHashMap<>();
                lastYearPozosProduccion.clear();
                lastProduccionDiariaPromedio.clear();
                ObjectMapper mapper = new ObjectMapper();
                double maxInfraDES = Double.MIN_VALUE;
                double maxPerfDES = Double.MIN_VALUE;
                double maxTermDES = Double.MIN_VALUE;
                double maxPCE = Double.MIN_VALUE;

                double maxPlataformadesarrollo = Double.MIN_VALUE;
                double maxEstacioncompresion = Double.MIN_VALUE;
                double maxDucto = Double.MIN_VALUE;
                double maxBateria = Double.MIN_VALUE;
                double maxManifolds = Double.MIN_VALUE;
                double maxRisers = Double.MIN_VALUE;
                double maxCubiertaDeProceso = Double.MIN_VALUE;
                FactorCalculoForMulti factorCalculoForMulti = new FactorCalculoForMulti();
                factorCalculoForMulti.setIsMulti(false);
                for (int i = 0; i < paramList.size(); i++) {
                        List<Object> params = paramList.get(i);

                        double currentPCE = getDoubleValue(params, 4);
                        double currentInfraDES = getDoubleValue(params, 11);
                        double currentPerfDES = getDoubleValue(params, 12);
                        double currentTermDES = getDoubleValue(params, 13);

                        double currentDucto = getDoubleValue(params, 9);
                        double currentBateria = getDoubleValue(params, 10);
                        double currentPlataformadesarrollo = getDoubleValue(params, 6);
                        double currentEstacioncompresion = getDoubleValue(params, 8);
                        double currentManifolds = getDoubleValue(params, 18);
                        double currentRisers = getDoubleValue(params, 19);
                        double currentCubiertaDeProceso = getDoubleValue(params, 21);

                        sumPCE += currentPCE;

                        if (currentPCE > maxPCE) {
                                maxPCE = currentPCE;
                        }
                        if (currentInfraDES > maxInfraDES) {
                                maxInfraDES = currentInfraDES;
                        }
                        if (currentPerfDES > maxPerfDES) {
                                maxPerfDES = currentPerfDES;
                        }
                        if (currentTermDES > maxTermDES) {
                                maxTermDES = currentTermDES;
                        }
                        if (currentDucto > maxDucto) {
                                maxDucto = currentDucto;
                        }
                        if (currentBateria > maxBateria) {
                                maxBateria = currentBateria;
                        }
                        if (currentPlataformadesarrollo > maxPlataformadesarrollo) {
                                maxPlataformadesarrollo = currentPlataformadesarrollo;
                        }
                        if (currentEstacioncompresion > maxEstacioncompresion) {
                                maxEstacioncompresion = currentEstacioncompresion;
                        }
                        if (currentManifolds > maxManifolds) {
                                maxManifolds = currentManifolds;
                        }
                        if (currentRisers > maxRisers) {
                                maxRisers = currentRisers;
                        }
                        if (currentCubiertaDeProceso > maxCubiertaDeProceso) {
                                maxCubiertaDeProceso = currentCubiertaDeProceso;
                        }
                        if (currentCubiertaDeProceso > maxCubiertaDeProceso) {
                                maxCubiertaDeProceso = currentCubiertaDeProceso;
                        }

                }
                List<Integer> idOportunidadObjetivoList = new ArrayList<>();
                for (int i = 0; i < paramList.size(); i++) {
                        List<Object> params = paramList.get(i);
                        String fechaToUse = (i == 0) ? "unexist" : currentFecha;
                        int oldPozosTerminadosValueToUse = (i == 0) ? 0 : currentOldPozosTerminadosValue;
                        int idOportunidadObjetivo = getIntValue(params, 0);

                        if (i == 0) {
                                // System.err.println("======================");
                                Object checkPCE0 = paramList.get(i + 0).get(4);
                                Object checkPCE1 = paramList.get(i + 1).get(4);
                                if (checkPCE0 instanceof String) {
                                        System.out.println("0st idOportunidadObjetivo : " + idOportunidadObjetivo);
                                        System.out.println("checkPCE0 : " + checkPCE0);
                                }
                                if (checkPCE1 instanceof String) {
                                        System.out.println("1st idOportunidadObjetivo : " + getIntValue(params, 1));
                                        System.out.println("checkPCE1 : " + checkPCE1);
                                }
                        }

                        double pce = parseToDouble(params.get(4));
                        double nextOrPreviewPce = 0.0;
                        if (i == 0) {
                                nextOrPreviewPce = parseToDouble(paramList.get(i + 1).get(4));
                        } else {
                                nextOrPreviewPce = parseToDouble(paramList.get(i - 1).get(4));
                        }

                        idOportunidadObjetivoList.add(idOportunidadObjetivo);
                        if (i != 0 && pce == 0.0) {
                                break;
                        }
                        EvaluacionResponse response = getInfoPozosService(
                                        i,
                                        idOportunidadObjetivo, // idOportunidadObjetivo
                                        getIntValue(params, 1), // version
                                        getDoubleValue(params, 2), // cuota
                                        getDoubleValue(params, 3), // declinada
                                        pce, // pce 
                                        getDoubleValue(params, 5), // area
                                        maxPlataformadesarrollo, // plataformadesarrollo
                                        getDoubleValue(params, 7), // lineadedescarga
                                        maxEstacioncompresion, // estacioncompresion
                                        maxDucto, // ducto
                                        maxBateria, // bateria
                                        maxInfraDES, // maxInfraDES
                                        maxPerfDES, // maxPerfDES
                                        maxTermDES, // maxTermDES
                                        getDoubleValue(params, 14), // infraDES
                                        getDoubleValue(params, 15), // perfDES
                                        getDoubleValue(params, 16), // termDES
                                        getDoubleValue(params, 17), // arbolesSubmarinos
                                        maxManifolds, // manifolds
                                        maxRisers, // risers
                                        getDoubleValue(params, 20), // sistemaDeControl
                                        maxCubiertaDeProceso, // cubiertaDeProceso
                                        getDoubleValue(params, 22), // buquetaqueCompra
                                        getDoubleValue(params, 23), // buquetaqueRenta
                                        fechaToUse, // fechaToUse
                                        oldPozosTerminadosValueToUse,
                                        lastYearPozosProduccion, lastProduccionDiariaPromedio);

                        if (i == 0 && nextOrPreviewPce == 0) {
                                return response;
                        }

                        if (response.getFactorCalculo() != null && pce != 0 && nextOrPreviewPce != 0) {
                                FactorCalculo eachFactorCalculo = response.getFactorCalculo();
                                // init
                                factorCalculoForMulti.setIsMulti(true);
                                factorCalculoForMulti.setSumFc_aceite(
                                                Objects.requireNonNullElse(factorCalculoForMulti.getSumFc_aceite(),
                                                                BigDecimal.ZERO));
                                factorCalculoForMulti.setSumFc_gas(
                                                Objects.requireNonNullElse(factorCalculoForMulti.getSumFc_gas(),
                                                                BigDecimal.ZERO));
                                factorCalculoForMulti.setSumFc_condensado(
                                                Objects.requireNonNullElse(factorCalculoForMulti.getSumFc_condensado(),
                                                                BigDecimal.ZERO));
                                BigDecimal pceBD = BigDecimal.valueOf(pce);

                                // set
                                factorCalculoForMulti.setSumFc_aceite(
                                                factorCalculoForMulti.getSumFc_aceite().add(
                                                                (eachFactorCalculo.getFc_aceite() != null
                                                                                ? BigDecimal.valueOf(eachFactorCalculo
                                                                                                .getFc_aceite())
                                                                                                .multiply(pceBD)
                                                                                : BigDecimal.ZERO)));

                                factorCalculoForMulti.setSumFc_gas(
                                                factorCalculoForMulti.getSumFc_gas().add(
                                                                (eachFactorCalculo.getFc_gas() != null
                                                                                ? BigDecimal.valueOf(eachFactorCalculo
                                                                                                .getFc_gas())
                                                                                                .multiply(pceBD)
                                                                                : BigDecimal.ZERO)));

                                factorCalculoForMulti.setSumFc_condensado(
                                                factorCalculoForMulti.getSumFc_condensado().add(
                                                                (eachFactorCalculo.getFc_condensado() != null
                                                                                ? BigDecimal.valueOf(eachFactorCalculo
                                                                                                .getFc_condensado())
                                                                                                .multiply(pceBD)
                                                                                : BigDecimal.ZERO)));
                                BigDecimal sumPceBD = BigDecimal.valueOf(sumPCE);
                                factorCalculoForMulti.setSumPce(sumPceBD);

                                response.setFactorCalculoForMulti(factorCalculoForMulti);

                        }

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

                                        if (pce != 0 && nextOrPreviewPce != 0) {
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
                        try {
                                mapper.writeValue(new File("response_each_" + i + "___.json"),
                                                response);
                        } catch (IOException e) {
                                log.error("Error saving merged responses to JSON", e);
                        }
                        responses.add(response);
                }

                if (responses.size() == 1) {
                        return responses.get(0);
                }

                double produccionTotalSum = 0;
                for (int i = 0; i < produccionTotalMmbpceArray.length; i++) {
                        produccionTotalSum += produccionTotalMmbpceArray[i];
                }
                produccionTotalMmbpceArrayThreadLocal.remove();

                ProduccionTotalMmbpce produccionTotalMmbpceParam = new ProduccionTotalMmbpce();
                produccionTotalMmbpceParam.setProduccionTotalMmbpce(produccionTotalSum);
                EvaluacionResponse mergedResult = evaluacionMerger.mergeEvaluaciones(responses,
                                produccionTotalMmbpceArray, sumPCE, maxPCE, factorCalculoForMulti);

                try {
                        mapper.writeValue(new File("all_responses.json"), mergedResult);
                } catch (IOException e) {
                        log.error("Error saving merged responses to JSON", e);
                }
                return mergedResult;
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

        private double parseToDouble(Object value) {
                if (value instanceof Number) {
                        return ((Number) value).doubleValue();
                } else if (value instanceof String) {
                        try {
                                return Double.parseDouble((String) value);
                        } catch (NumberFormatException e) {
                                log.error("Invalid number format: {}", value, e);
                        }
                }
                return 0.0;
        }

}
