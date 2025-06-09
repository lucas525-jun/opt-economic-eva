package com.pemex.pep.seemop.orchestrator.services;

import java.math.BigDecimal;
import java.time.Year;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.IntStream;

import com.pemex.pep.seemop.data.models.*;
import com.pemex.pep.seemop.data.models.CostoOperacion;
import com.pemex.pep.seemop.data.models.FactorInversion;
import com.pemex.pep.seemop.data.models.FactorInversionDesarrollo;
import com.pemex.pep.seemop.data.models.FactorInversionExploratorio;
import com.pemex.pep.seemop.data.models.InformacionInversion;
import com.pemex.pep.seemop.data.models.Paridad;
import com.pemex.pep.seemop.data.models.PozosActivos;
import com.pemex.pep.seemop.data.models.PrecioHidrocarburo;
import com.pemex.pep.seemop.data.models.ProduccionTotalMmbpce;
import com.pemex.pep.seemop.data.models.VectorProduccion;
import com.pemex.pep.seemop.orchestrator.models.*;
import io.quarkus.logging.Log;
import com.pemex.pep.seemop.data.entities.InformacionOportunidad;
import mx.com.innovating.cloud.data.models.*;
import com.pemex.pep.seemop.data.repository.DataBaseConnectorRepository;
import mx.com.innovating.cloud.orchestrator.models.*;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import lombok.extern.slf4j.Slf4j;
import com.pemex.pep.seemop.orchestrator.utilities.DataProcess;

@Slf4j
@ApplicationScoped
public class EvaluacionEconomicaService {

        @Inject
        DataBaseConnectorRepository databaseConnectorClient;

        public EvaluacionResponse getInfoPozosService(
                        Integer idOportunidadObjetivo,
                        Integer version,
                        double cuota, double declinada, double pce, double area, double plataformadesarrollo,
                        double lineadedescarga, double estacioncompresion,
                        double ducto, double bateria, double infra, double perf,
                        double term, double infraDES, double perfDES, double termDES, double arbolesSubmarinos,
                        double manifolds, double risers, double sistemaDeControl, double cubiertaDeProceso,
                        double buquetaqueCompra, double buquetaqueRenta) {

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

                        // Lógica de preparación de evaluación económica dentro del `if`
                        FactorInversionExploratorio fiExploratoria = new FactorInversionExploratorio();
                        fiExploratoria.setInfraestructura(infra);
                        fiExploratoria.setPerforacion(perf);
                        fiExploratoria.setTerminacion(term);

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
                                        flujosContablesTotales, null);

                } else {

                        // log.info("PCE distinto de 0. Ejecutando flujo completo.");

                        // log.info(" 1 / 12 - getPozoPerforados");
                        InformacionOportunidad oportunity = databaseConnectorClient
                                        .getInfoOportunidad(idOportunidadObjetivo);

                        if (oportunity == null) {
                                Log.error("InformacionOportunidad is null");
                                throw new RuntimeException("InformacionOportunidad is null");
                        }

                        // log.info(" 2 / 12 - getPozoPerforados");
                        List<EscaleraProduccionMulti> listTerminados = databaseConnectorClient.getPozosPerforados(
                                        idOportunidadObjetivo, version, cuota, declinada, pce, area); // getPozosTerminados(idOportunidad)
                                                                                                      // hecho

                        // log.info(" 3 / 12 - getFactorInversionExploratorio");
                        FactorInversionExploratorio fiExploratoria = new FactorInversionExploratorio();
                        fiExploratoria.setInfraestructura(infra);
                        fiExploratoria.setPerforacion(perf);
                        fiExploratoria.setTerminacion(term);
                        // log.info("-------------------------------------------------");
                        // log.info(fiExploratoria);

                        // log.info(" 4 / 12 - getFactorInversionDesarrollo");

                        FactorInversionDesarrollo fiDesarrollo = new FactorInversionDesarrollo(); // enviar de micro
                        fiDesarrollo.setInfraestructura(infraDES);
                        fiDesarrollo.setPerforacion(perfDES);
                        fiDesarrollo.setTerminacion(termDES);

                        // log.info(" 5 / 12 - getInformacionInversion");
                        InformacionInversion infoInversion = new InformacionInversion();
                        infoInversion.setPlataformadesarrollo(plataformadesarrollo);
                        infoInversion.setLineadedescarga(lineadedescarga);
                        infoInversion.setEstacioncompresion(estacioncompresion);
                        infoInversion.setDucto(ducto);
                        infoInversion.setBateria(bateria);
                        infoInversion.setArbolessubmarinos(arbolesSubmarinos);
                        infoInversion.setManifolds(manifolds);
                        infoInversion.setRisers(risers);
                        infoInversion.setSistemasdecontrol(sistemaDeControl);
                        infoInversion.setCubiertadeproces(cubiertaDeProceso);
                        infoInversion.setBuquetanquecompra(buquetaqueCompra);
                        infoInversion.setBuquetanquerenta(buquetaqueRenta);

                        // log.info(" 6 / 12 - getCostoOperacion");
                        List<CostoOperacion> listCostoOperacion = databaseConnectorClient
                                        .getCostoOperacion(idOportunidadObjetivo);

                        // log.info(" 7 / 12 - getProduccionTotalMmbpce");
                        ProduccionTotalMmbpce produccionTotalMmbpce = databaseConnectorClient.getProduccionTotalMmbpce(
                                        idOportunidadObjetivo, version, cuota, declinada, pce, area);

                        // log.info(" 8 / 12 - getParidad");
                        Paridad paridad = databaseConnectorClient
                                        .getParidad(Integer.valueOf(oportunity.getFechainicioperfexploratorio()));

                        // log.info(" 9 / 12 - getVectorProduccion");
                        List<VectorProduccion> listVectorProduccion = databaseConnectorClient.getVectorProduccion(
                                        idOportunidadObjetivo, version, cuota, declinada, pce, area);

                        // log.info(" 10 / 12 - getPrecioHidrocarburo");
                        List<PrecioHidrocarburo> listPrecios = databaseConnectorClient
                                        .getPrecioHidrocarburo(idOportunidadObjetivo, oportunity.getIdprograma(), version);

                        // log.info(" 11 / 12 - getPozosActivos");
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

                        // log.info(" 12 / 12 - getFactorInversion");
                        FactorInversion factorInversion = databaseConnectorClient
                                        .getFactorInversion(idOportunidadObjetivo);

                        // log.info(" 13 / 13 - factor calculo");
                        FactorCalculo factorCalculo = databaseConnectorClient.getFactorCalculo(idOportunidadObjetivo,
                                        version);

                        CalculoNumPozosResult calculoPozosTotales = databaseConnectorClient.getPozosTotales(version, idOportunidadObjetivo, cuota, declinada, pce, area);


                        // log.info("Obteniendo la informacion de los pozos perforados");
                        assert listTerminados != null;
                        Map<Integer, BigDecimal> pozosPerforados = DataProcess.getPozosPerforadosByAnio(listTerminados,
                                        oportunity.getFechainicio());

                        // log.info("::::: numero de anios con pozos perforados: {}",
                        // pozosPerforados.size());

                        // log.info("Obteniendo la informacion de los pozos terminados");
                        Map<Integer, BigDecimal> pozosTerminados = DataProcess.getPozosterminadosByAnio(listTerminados);

                        // log.info("::::: numero de anios con pozos terminados: {}",
                        // pozosTerminados.size());

                        // log.info("Obteniendo la informacion del plan de desarrollo");
                        List<OportunidadPlanDesarrollo> planDesarrollo = databaseConnectorClient
                                        .getPlanDesarrolloByOportunidad(idOportunidadObjetivo, version);

                        var anioFinal = Integer.parseInt(oportunity.getFechainicio());
                        if (pozosPerforados.size() > 1) {
                                for (Integer key : pozosTerminados.keySet()) {
                                        anioFinal = anioFinal > key ? anioFinal : key;
                                }
                                // log.info("::::: anioFinal: {}", anioFinal);
                        }

                        // log.info("Obteniendo vector de produccion");
                        Map<String, Double> vectorProduccion = new HashMap<>();
                        assert listVectorProduccion != null;
                        listVectorProduccion.forEach(item -> vectorProduccion.put(String.valueOf(item.getAanio()),
                                        item.getCtotalanual()));

                        // log.info("Calculando produccion diaria promedio");
                        Map<String, ProduccionDiariaPromedio> produccionDiariaPromedio = DataProcess
                                        .calculaProduccionDiariaPromedioByAnio(factorInversion, vectorProduccion,
                                                        oportunity.getIdhidrocarburo());

                        // log.info("Obteniendo precios hidrocarburos");
                        Map<String, Double> preciosMap = new HashMap<>();
                        assert listPrecios != null;
                        listPrecios.forEach(
                                        item -> preciosMap.put(item.getFecha() + "-" + item.getIdhidrocarburo(),
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
                        int finalAnioFinal = anioFinal;
                        assert listActivos != null;
                        listActivos.forEach(item -> {
                                var anioActualInteger = Integer.parseInt(item.getAnio());
                                int yearDays = Year.of(anioActualInteger).length();
                                BigDecimal perforado = new BigDecimal(0);
                                BigDecimal terminado = new BigDecimal(0);

                                Integer aniosPerforacion = pozosPerforados.size();

                                var inversionesAnioActual = new Inversiones();
                                if (pozosTerminados.containsKey(anioActualInteger)) {
                                        terminado = pozosTerminados.get(anioActualInteger);
                                }
                                if (pozosPerforados.containsKey(anioActualInteger) && item.getAnio().equals(oportunity.getFechainicio())) {

                                        perforado = pozosPerforados.get(anioActualInteger).subtract(new BigDecimal(1));

                                        assert fiExploratoria != null;
                                        var invExploratoria = DataProcess.calculaInversionExploratoria(fiExploratoria,
                                                        paridad.getParidad());

                                        /****************************************************************************************************/

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
                                                                        oportunity.getFechainicioperfexploratorio(),
                                                                        null, null, null,
                                                                        inversionesExpAnioInicioPerf, null, null));

                                        /****************************************************************************************************/

                                        assert terminado != null;
                                        assert fiDesarrollo != null;
                                        var invDesarrollo = DataProcess.calculaInversionDesarrollo(fiDesarrollo,
                                                        paridad.getParidad(), terminado, perforado, aniosPerforacion);

                                        /***************************************************************************************************/

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

                                        var inversionesAnioAnterior = new Inversiones();

                                        if (!existe.get()) {
                                                /*
                                                 * var inversionesAnioAnterior = new Inversiones(null,
                                                 * null, null, null, null,
                                                 * invDesarrollo.getDesarrolloSinOperacional(),
                                                 * invDesarrollo.getPerforacionDes(),
                                                 * invDesarrollo.getTerminacionDes(),
                                                 * invDesarrollo.getInfraestructuraDes(), null,
                                                 * null, null, null, invDesarrollo.getDesarrolloSinOperacional());
                                                 */
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

                                        /***************************************************************************************************/

                                        // if (finalAnioFinal == anioActualInteger) {
                                        // var totalInversionAnioAnterior = invDesarrollo.getDesarrolloSinOperacional()
                                        // + invExploratoria.getExploratoria();
                                        // var inversionesAnioAnterior = new Inversiones(totalInversionAnioAnterior,
                                        // invExploratoria.getExploratoria(), invExploratoria.getPerforacionExp(),
                                        // invExploratoria.getTerminacionExp(), invExploratoria.getInfraestructuraExp(),
                                        // invDesarrollo.getDesarrolloSinOperacional(),
                                        // invDesarrollo.getPerforacionDes(),
                                        // invDesarrollo.getTerminacionDes(), invDesarrollo.getInfraestructuraDes(),
                                        // null,
                                        // null, null, null, invDesarrollo.getDesarrolloSinOperacional());
                                        //
                                        // var anioAnteriorInversion = Integer.parseInt(item.getAnio()) - 1;
                                        // evaluacionEconomica.add(new
                                        // EvaluacionEconomica(Integer.toString(anioAnteriorInversion),
                                        // null, null, null, inversionesAnioAnterior, null, null));
                                        // } else {

                                        // var inversionesExploratoriasAnioAnterior = new Inversiones(
                                        // invExploratoria.getExploratoria(), invExploratoria.getExploratoria(),
                                        // invExploratoria.getPerforacionExp(), invExploratoria.getTerminacionExp(),
                                        // invExploratoria.getInfraestructuraExp(), null, null, null,
                                        // null, null, null, null, null, null);
                                        //
                                        // evaluacionEconomica.add(
                                        // new EvaluacionEconomica(oportunity.getFechainicioperfexploratorio(),
                                        // null, null, null,
                                        // inversionesExploratoriasAnioAnterior, null, null));
                                        //
                                        // var inversionesDesarrolloAnioAnterior = new Inversiones(
                                        // invDesarrollo.getDesarrolloSinOperacional(), null, null, null, null,
                                        // invDesarrollo.getDesarrolloSinOperacional(),
                                        // invDesarrollo.getPerforacionDes(),
                                        // invDesarrollo.getTerminacionDes(), invDesarrollo.getInfraestructuraDes(),
                                        // null,
                                        // null, null, null, invDesarrollo.getDesarrolloSinOperacional());
                                        //
                                        // var anioAnteriorInversionDesarrollo = Integer.parseInt(item.getAnio()) - 1;
                                        // evaluacionEconomica
                                        // .add(new
                                        // EvaluacionEconomica(Integer.toString(anioAnteriorInversionDesarrollo),
                                        // null, null, null, inversionesDesarrolloAnioAnterior, null, null));
                                        // }
                                        double pozosTotales = calculoPozosTotales.getNPozos();
                                        assert infoInversion != null;

                                        double cantManifolds = Math.ceil(pozosTotales / 6.0);
                                        double ductos = 0;
                                        double plataformasDesarrollo = 0;

                                        double duracionMax = planDesarrollo.stream().mapToDouble(OportunidadPlanDesarrollo::getDuracion).max().orElseThrow(() -> new NoSuchElementException("La lista está vacía"));

                                        var anioInicioPerfexploratorio = Integer.parseInt(oportunity.getFechainicioperfexploratorio());
                                        var anioInicio = Integer.parseInt(oportunity.getFechainicio());
                                        for (OportunidadPlanDesarrollo plan : planDesarrollo) {
                                                if (duracionMax >= 2) { // Para versiones 2 o superiores
                                                        if (anioInicioPerfexploratorio + plan.getDuracion() == anioInicio) {
                                                                ductos = infoInversion.getDucto() * paridad.getParidad();
                                                                plataformasDesarrollo = infoInversion.getPlataformadesarrollo() * paridad.getParidad();
                                                                inversionesAnioAnterior.setDuctos(ductos);
                                                                inversionesAnioAnterior.setPlataformaDesarrollo(plataformasDesarrollo);
                                                                var sistemaDeControlG = infoInversion.getSistemasdecontrol() * paridad.getParidad() * cantManifolds;
                                                                inversionesAnioAnterior.setSistemaDeControl(sistemaDeControlG);
                                                                var cubiertaProcesosG = infoInversion.getCubiertadeproces() * paridad.getParidad() * cantManifolds;
                                                                inversionesAnioAnterior.setCubiertaProcesos(cubiertaProcesosG);
                                                                var risersG = infoInversion.getRisers() * paridad.getParidad() * cantManifolds;
                                                                inversionesAnioAnterior.setRisers(risersG);
                                                        } else {
                                                                var arbolesSubmarinosG = infoInversion.getArbolessubmarinos() * paridad.getParidad() * pozosTotales;
                                                                inversionesAnioActual.setArbolSubmarinos(arbolesSubmarinosG);
                                                                var manifoldsG = infoInversion.getManifolds() * paridad.getParidad() * cantManifolds;
                                                                inversionesAnioActual.setManifolds(manifoldsG);
                                                                var estacionCompresionG = infoInversion.getEstacioncompresion() * paridad.getParidad();
                                                                inversionesAnioActual.setEstacionCompresion(estacionCompresionG);
                                                                var bateriaG = infoInversion.getBateria() * paridad.getParidad();
                                                                inversionesAnioActual.setBateria(bateriaG);
                                                                var buqueTanqueCompraG = infoInversion.getBuquetanquecompra() * paridad.getParidad();
                                                                inversionesAnioActual.setBuqueTanqueCompra(buqueTanqueCompraG);
                                                                var buqueTanqueRentaG = infoInversion.getBuquetanquerenta() * paridad.getParidad();
                                                                inversionesAnioActual.setBuqueTanqueRenta(buqueTanqueRentaG);
                                                        }
                                                } else {
                                                        ductos = infoInversion.getDucto() * paridad.getParidad();
                                                        plataformasDesarrollo = infoInversion.getPlataformadesarrollo() * paridad.getParidad();
                                                        if(anioInicioPerfexploratorio + plan.getDuracion() == anioInicio){
                                                                inversionesAnioAnterior.setDuctos(ductos);
                                                                inversionesAnioAnterior.setPlataformaDesarrollo(plataformasDesarrollo);
                                                                var arbolesSubmarinosG = infoInversion.getArbolessubmarinos() * paridad.getParidad() * pozosTotales;
                                                                inversionesAnioActual.setArbolSubmarinos(arbolesSubmarinosG);
                                                                var manifoldsG = infoInversion.getManifolds() * paridad.getParidad() * cantManifolds;
                                                                inversionesAnioActual.setManifolds(manifoldsG);
                                                        }
                                                        inversionesAnioActual.setDuctos(ductos);
                                                        inversionesAnioActual.setPlataformaDesarrollo(plataformasDesarrollo);
                                                }
                                        }

                                        var lineaDescarga = infoInversion.getLineadedescarga() * terminado.doubleValue() * paridad.getParidad();

                                        var futuroDesarrollo = (costoOperacionMap.get(item.getAnio() + "-19")
                                                        * produccionDiariaPromedio.get(item.getAnio()).getMbpce()
                                                        * yearDays
                                                        * paridad.getParidad()) / 1000;

                                        var mantenimientoPozos = (costoOperacionMap.get(item.getAnio() + "-25")
                                                        * produccionDiariaPromedio.get(item.getAnio()).getMbpce()
                                                        * yearDays
                                                        * paridad.getParidad())/ 1000;

                                        var mantenimientoInfraestructuraFutDes = (costoOperacionMap
                                                        .get(item.getAnio() + "-23")
                                                        * produccionDiariaPromedio.get(item.getAnio()).getMbpce()
                                                        * yearDays
                                                        * paridad.getParidad())/ 1000;
                                        /*
                                         * if(ductos == 0){
                                         * ductos = infoInversion.getDucto() * paridad.getParidad();
                                         * inversionesAnioActual.setDuctos(ductos);
                                         * }
                                         */

                                        inversionesAnioActual.setLineaDescarga(lineaDescarga);
                                        inversionesAnioActual.setOperacionalFuturoDesarrollo(futuroDesarrollo);
                                        inversionesAnioActual.setMantenimientoDePozos(mantenimientoPozos);
                                        inversionesAnioActual.setMantenimientoInfraestructuraFuturoDesarrollo(
                                                        mantenimientoInfraestructuraFutDes);

                                } else if (pozosPerforados.containsKey(anioActualInteger)) {
                                        perforado = pozosPerforados.get(anioActualInteger);

                                        assert terminado != null;
                                        var terminadoFinal = terminado;
                                        if (anioActualInteger == finalAnioFinal) {
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
                                        var futuroDesarrollo = (costoOperacionMap.get(item.getAnio() + "-19")
                                                        * produccionDiariaPromedio.get(item.getAnio()).getMbpce()
                                                        * yearDays
                                                        * paridad.getParidad()) / 1000;

                                        var mantenimientoPozos = (costoOperacionMap.get(item.getAnio() + "-25")
                                                        * produccionDiariaPromedio.get(item.getAnio()).getMbpce()
                                                        * yearDays
                                                        * paridad.getParidad()) / 1000;

                                        var mantenimientoInfraestructuraFutDes = (costoOperacionMap
                                                        .get(item.getAnio() + "-23")
                                                        * produccionDiariaPromedio.get(item.getAnio()).getMbpce()
                                                        * yearDays
                                                        * paridad.getParidad()) /1000;

                                        inversionesAnioActual.setLineaDescarga(lineaDescarga);
                                        inversionesAnioActual.setOperacionalFuturoDesarrollo(futuroDesarrollo);
                                        inversionesAnioActual.setMantenimientoDePozos(mantenimientoPozos);
                                        inversionesAnioActual.setMantenimientoInfraestructuraFuturoDesarrollo(
                                                        mantenimientoInfraestructuraFutDes);
                                } else if (pozosTerminados.containsKey(anioActualInteger)) {

                                        terminado = pozosTerminados.get(anioActualInteger);

                                        var terminadoFinal = terminado;

                                        if (anioActualInteger == finalAnioFinal) {
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
                                        var futuroDesarrollo = (costoOperacionMap.get(item.getAnio() + "-19")
                                                        * produccionDiariaPromedio.get(item.getAnio()).getMbpce()
                                                        * yearDays
                                                        * paridad.getParidad()) / 1000;

                                        var mantenimientoPozos = (costoOperacionMap.get(item.getAnio() + "-25")
                                                        * produccionDiariaPromedio.get(item.getAnio()).getMbpce()
                                                        * yearDays
                                                        * paridad.getParidad()) / 1000;

                                        var mantenimientoInfraestructuraFutDes = (costoOperacionMap
                                                        .get(item.getAnio() + "-23")
                                                        * produccionDiariaPromedio.get(item.getAnio()).getMbpce()
                                                        * yearDays
                                                        * paridad.getParidad()) / 1000;

                                        inversionesAnioActual.setLineaDescarga(lineaDescarga);
                                        inversionesAnioActual.setOperacionalFuturoDesarrollo(futuroDesarrollo);
                                        inversionesAnioActual.setMantenimientoDePozos(mantenimientoPozos);
                                        inversionesAnioActual.setMantenimientoInfraestructuraFuturoDesarrollo(
                                                        mantenimientoInfraestructuraFutDes);

                                }

                                evaluacionEconomica.add(new EvaluacionEconomica(item.getAnio(),
                                                new Pozo(item.getPromedioAnual(), perforado, terminado),
                                                produccionDiariaPromedio.get(item.getAnio()),
                                                ingresosMap.get(item.getAnio()),
                                                inversionesAnioActual, costosMap.get(item.getAnio()), null));

                        });

                        DataProcess.finalProcessInversiones(evaluacionEconomica);

                        DataProcess.calculaFlujoContable(evaluacionEconomica);

                        evaluacionEconomica.sort(Comparator.comparing(EvaluacionEconomica::getAnio));

                        var flujosContablesTotales = DataProcess.calculaFlujosContablesTotales(evaluacionEconomica,
                                        produccionTotalMmbpce, factorInversion, pce);

                        Double areamasignacion = databaseConnectorClient
                                        .getAreakmasignacion(idOportunidadObjetivo, version).getAreakmasignacion();

                        return new EvaluacionResponse(areamasignacion, pce, oportunity, evaluacionEconomica,
                                        flujosContablesTotales, factorCalculo);

                }

        }
}
