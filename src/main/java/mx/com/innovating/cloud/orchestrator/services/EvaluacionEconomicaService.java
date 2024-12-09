package mx.com.innovating.cloud.orchestrator.services;

import java.math.BigDecimal;
import java.time.Year;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import io.quarkus.logging.Log;
import mx.com.innovating.cloud.data.entities.InformacionOportunidad;
import mx.com.innovating.cloud.data.models.*;
import mx.com.innovating.cloud.data.models.CostoOperacion;
import mx.com.innovating.cloud.data.models.EscaleraProduccion;
import mx.com.innovating.cloud.data.models.FactorInversion;
import mx.com.innovating.cloud.data.models.FactorInversionDesarrollo;
import mx.com.innovating.cloud.data.models.FactorInversionExploratorio;
import mx.com.innovating.cloud.data.models.InformacionInversion;
import mx.com.innovating.cloud.data.models.Paridad;
import mx.com.innovating.cloud.data.models.PozosActivos;
import mx.com.innovating.cloud.data.models.PrecioHidrocarburo;
import mx.com.innovating.cloud.data.models.ProduccionTotalMmbpce;
import mx.com.innovating.cloud.data.models.VectorProduccion;
import mx.com.innovating.cloud.data.repository.DataBaseConnectorRepository;
import mx.com.innovating.cloud.orchestrator.models.*;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import lombok.extern.slf4j.Slf4j;
import mx.com.innovating.cloud.orchestrator.utilities.DataProcess;

@Slf4j
@ApplicationScoped
public class EvaluacionEconomicaService {

    @Inject
    DataBaseConnectorRepository databaseConnectorClient;

    public EvaluacionResponse getInfoPozosService(
            Integer idOportunidadObjetivo,
            Integer version,
            double cuota, double declinada, double pce, double area, double plataformadesarrollo, double lineadedescarga, double estacioncompresion,
            double ducto, double bateria, double infra, double perf,
            double term, double infraDES, double perfDES, double termDES, double arbolesSubmarinos,
            double manifolds, double risers, double sistemaDeControl, double cubiertaDeProceso, double buquetaqueCompra, double buquetaqueRenta
    ) {

        if (pce == 0) {
            InformacionOportunidad oportunity = databaseConnectorClient.getInfoOportunidad(idOportunidadObjetivo);
            List<EvaluacionEconomica> evaluacionEconomica;

            Log.info("PCE es 0. Ejecutando cálculos de inversión exploratoria y flujo contable.");

            Log.info(" 12 / 12 - getFactorInversion");
            FactorInversion factorInversion = databaseConnectorClient.getFactorInversion(idOportunidadObjetivo);
            factorInversion.setPce(0.0);

            Log.info("  7 / 12 - getProduccionTotalMmbpce");
            ProduccionTotalMmbpce produccionTotalMmbpce = databaseConnectorClient.getProduccionTotalMmbpce(idOportunidadObjetivo, version, cuota, declinada, pce, area);


            // Lógica de preparación de evaluación económica dentro del `if`
            FactorInversionExploratorio fiExploratoria = new FactorInversionExploratorio();
            fiExploratoria.setInfraestructura(infra);
            fiExploratoria.setPerforacion(perf);
            fiExploratoria.setTerminacion(term);

            Paridad paridad = databaseConnectorClient.getParidad(
                    Integer.valueOf(oportunity.getFechainicioperfexploratorio())
            );
            var invExploratoria = DataProcess.calculaInversionExploratoria(fiExploratoria, paridad.getParidad());

            evaluacionEconomica = new ArrayList<>();
            var inversionesExpAnioInicioPerf = new Inversiones(
                    null, invExploratoria.getExploratoria(), invExploratoria.getPerforacionExp(),
                    invExploratoria.getTerminacionExp(), invExploratoria.getInfraestructuraExp(), 0.0, 0.0,
                    0.0, 0.0, 0.0, 0.0, 0.0, 0.0, 0.0
            );

            evaluacionEconomica.add(
                    new EvaluacionEconomica(
                            oportunity.getFechainicioperfexploratorio(),
                            null, null, null, inversionesExpAnioInicioPerf, null, null
                    )
            );

            // **Agregar flujo contable**
            DataProcess.finalProcessInversiones(evaluacionEconomica);
            DataProcess.calculaFlujoContable(evaluacionEconomica);

            var flujosContablesTotales = DataProcess.calculaFlujosContablesTotales(
                    evaluacionEconomica, produccionTotalMmbpce, factorInversion, pce
            );

            return new EvaluacionResponse(oportunity, evaluacionEconomica, flujosContablesTotales, null);

        }  else {

            Log.info("PCE distinto de 0. Ejecutando flujo completo.");

            Log.info("  1 / 12 - getPozoPerforados");
            InformacionOportunidad oportunity = databaseConnectorClient.getInfoOportunidad(idOportunidadObjetivo);

            if (oportunity == null) {
                Log.error("InformacionOportunidad is null");
                throw new RuntimeException("InformacionOportunidad is null");
            }


            Log.info("  2 / 12 - getPozoPerforados");
            List<EscaleraProduccion> listTerminados = databaseConnectorClient.getPozosPerforados(idOportunidadObjetivo, version, cuota, declinada, pce, area); //getPozosTerminados(idOportunidad) hecho

            Log.info("  3 / 12 - getFactorInversionExploratorio");
            FactorInversionExploratorio fiExploratoria = new FactorInversionExploratorio();
            fiExploratoria.setInfraestructura(infra);
            fiExploratoria.setPerforacion(perf);
            fiExploratoria.setTerminacion(term);
            Log.info("-------------------------------------------------");
            Log.info(fiExploratoria);

            Log.info("  4 / 12 - getFactorInversionDesarrollo");

            FactorInversionDesarrollo fiDesarrollo = new FactorInversionDesarrollo();  // enviar de micro
            fiDesarrollo.setInfraestructura(infraDES);
            fiDesarrollo.setPerforacion(perfDES);
            fiDesarrollo.setTerminacion(termDES);

            Log.info("  5 / 12 - getInformacionInversion");
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



            Log.info("  6 / 12 - getCostoOperacion");
            List<CostoOperacion> listCostoOperacion = databaseConnectorClient.getCostoOperacion(oportunity.getIdproyecto());

            Log.info("  7 / 12 - getProduccionTotalMmbpce");
            ProduccionTotalMmbpce produccionTotalMmbpce = databaseConnectorClient.getProduccionTotalMmbpce(idOportunidadObjetivo, version, cuota, declinada, pce, area);

            Log.info("  8 / 12 - getParidad");
            Paridad paridad = databaseConnectorClient.getParidad(Integer.valueOf(oportunity.getFechainicioperfexploratorio()));

            Log.info("  9 / 12 - getVectorProduccion");
            List<VectorProduccion> listVectorProduccion = databaseConnectorClient.getVectorProduccion(idOportunidadObjetivo, version, cuota, declinada, pce, area);

            Log.info(" 10 / 12 - getPrecioHidrocarburo");
            List<PrecioHidrocarburo> listPrecios = databaseConnectorClient.getPrecioHidrocarburo(idOportunidadObjetivo, oportunity.getIdprograma());


            Log.info(" 11 / 12 - getPozosActivos");
            List<PozosActivos> listActivos = databaseConnectorClient.getPozosActivos(idOportunidadObjetivo, version, cuota, declinada,pce,area);

            // error
            Log.warn("Consultas que no funcionan");
            Log.info(" 12 / 12 - getFactorInversion");
            FactorInversion factorInversion = databaseConnectorClient.getFactorInversion(idOportunidadObjetivo);


            Log.info(" 13 / 13 - factor calculo");
            FactorCalculo factorCalculo = databaseConnectorClient.getFactorCalculo(idOportunidadObjetivo, version);


            log.info("Obteniendo la informacion de los pozos perforados");
            assert listTerminados != null;
            Map<Integer, BigDecimal> pozosPerforados = DataProcess.getPozosPerforadosByAnio(listTerminados,
                    oportunity.getFechainicio());

            log.info("::::: numero de anios con pozos perforados: {}", pozosPerforados.size());

            log.info("Obteniendo la informacion de los pozos terminados");
            Map<Integer, BigDecimal> pozosTerminados = DataProcess.getPozosterminadosByAnio(listTerminados);

            log.info("::::: numero de anios con pozos terminados: {}", pozosTerminados.size());

            log.info("Obteniendo la informacion del plan de desarrollo");
            List<OportunidadPlanDesarrollo> planDesarrollo = databaseConnectorClient.getPlanDesarrolloByOportunidad(idOportunidadObjetivo, version);


            var anioFinal = Integer.parseInt(oportunity.getFechainicio());
            if (pozosPerforados.size() > 1) {
                for (Integer key : pozosTerminados.keySet()) {
                    anioFinal = anioFinal > key ? anioFinal : key;
                }
                log.info("::::: anioFinal: {}", anioFinal);
            }

            log.info("Obteniendo vector de produccion");
            Map<String, Double> vectorProduccion = new HashMap<>();
            assert listVectorProduccion != null;
            listVectorProduccion.forEach(item -> vectorProduccion.put(String.valueOf(item.getAanio()), item.getCtotalanual()));

            log.info("Calculando produccion diaria promedio");
            Map<String, ProduccionDiariaPromedio> produccionDiariaPromedio = DataProcess
                    .calculaProduccionDiariaPromedioByAnio(factorInversion, vectorProduccion,
                            oportunity.getIdhidrocarburo());

            log.info("Obteniendo precios hidrocarburos");
            Map<String, Double> preciosMap = new HashMap<>();
            assert listPrecios != null;
            listPrecios.forEach(item -> preciosMap.put(item.getAnioprecio() + "-" + item.getIdhidrocarburo(),
                    item.getPrecio()));

            log.info("Calculando Ingresos");
            assert paridad != null;
            Map<String, Ingresos> ingresosMap = DataProcess.calculaIngresosByAnio(paridad,
                    produccionDiariaPromedio, preciosMap);

            Map<String, Double> costoOperacionMap = new HashMap<>();
            assert listCostoOperacion != null;
            listCostoOperacion.forEach(element -> costoOperacionMap
                    .put(element.getAnio() + "-" + element.getIdcostooperacion(), element.getGasto()));

            Map<String, Costos> costosMap = DataProcess.calculaCostosByAnio(costoOperacionMap,
                    produccionDiariaPromedio, paridad);

            log.info("Generando Respuesta");
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
                if (pozosPerforados.containsKey(anioActualInteger)
                        && item.getAnio().equals(oportunity.getFechainicio())) {

                    perforado = pozosPerforados.get(anioActualInteger).subtract(new BigDecimal(1));

                    assert fiExploratoria != null;
                    var invExploratoria = DataProcess.calculaInversionExploratoria(fiExploratoria,
                            paridad.getParidad());

                    /****************************************************************************************************/

                    var inversionesExpAnioInicioPerf = new Inversiones(
                            null, invExploratoria.getExploratoria(),
                            invExploratoria.getPerforacionExp(), invExploratoria.getTerminacionExp(),
                            invExploratoria.getInfraestructuraExp(), null, null, null,
                            null, null, null, null, null, null);

                    evaluacionEconomica.add(
                            new EvaluacionEconomica(oportunity.getFechainicioperfexploratorio(),
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
                                    .setDesarrolloSinOperacional(invDesarrollo.getDesarrolloSinOperacional());
                            evaluacion.getInversiones().setTerminacionDes(invDesarrollo.getTerminacionDes());
                            evaluacion.getInversiones().setPerforacionDes(invDesarrollo.getPerforacionDes());
                            evaluacion.getInversiones()
                                    .setInfraestructuraDes(invDesarrollo.getInfraestructuraDes());
                            evaluacion.getInversiones()
                                    .setDesarrollo(invDesarrollo.getDesarrollo());

                        }
                    });

                    var inversionesAnioAnterior = new Inversiones();

                    if(!existe.get()){
                      /*
                    var inversionesAnioAnterior = new Inversiones(null,
                            null, null, null, null,
                            invDesarrollo.getDesarrolloSinOperacional(), invDesarrollo.getPerforacionDes(),
                            invDesarrollo.getTerminacionDes(), invDesarrollo.getInfraestructuraDes(), null,
                            null, null, null, invDesarrollo.getDesarrolloSinOperacional());
                     */
                        inversionesAnioAnterior.setDesarrolloSinOperacional(invDesarrollo.getDesarrolloSinOperacional());
                        inversionesAnioAnterior.setPerforacionDes(invDesarrollo.getPerforacionDes());
                        inversionesAnioAnterior.setTerminacionDes(invDesarrollo.getTerminacionDes());
                        inversionesAnioAnterior.setInfraestructuraDes(invDesarrollo.getInfraestructuraDes());
                        inversionesAnioAnterior.setDesarrollo(invDesarrollo.getDesarrolloSinOperacional());

                        evaluacionEconomica.add(new EvaluacionEconomica(Integer.toString(anioCompare),
                                null, null, null, inversionesAnioAnterior, null, null));
                    }

                    /***************************************************************************************************/

//                if (finalAnioFinal == anioActualInteger) {
//                    var totalInversionAnioAnterior = invDesarrollo.getDesarrolloSinOperacional()
//                            + invExploratoria.getExploratoria();
//                    var inversionesAnioAnterior = new Inversiones(totalInversionAnioAnterior,
//                            invExploratoria.getExploratoria(), invExploratoria.getPerforacionExp(),
//                            invExploratoria.getTerminacionExp(), invExploratoria.getInfraestructuraExp(),
//                            invDesarrollo.getDesarrolloSinOperacional(), invDesarrollo.getPerforacionDes(),
//                            invDesarrollo.getTerminacionDes(), invDesarrollo.getInfraestructuraDes(), null,
//                            null, null, null, invDesarrollo.getDesarrolloSinOperacional());
//
//                    var anioAnteriorInversion = Integer.parseInt(item.getAnio()) - 1;
//                    evaluacionEconomica.add(new EvaluacionEconomica(Integer.toString(anioAnteriorInversion),
//                            null, null, null, inversionesAnioAnterior, null, null));
//                } else {

//                    var inversionesExploratoriasAnioAnterior = new Inversiones(
//                            invExploratoria.getExploratoria(), invExploratoria.getExploratoria(),
//                            invExploratoria.getPerforacionExp(), invExploratoria.getTerminacionExp(),
//                            invExploratoria.getInfraestructuraExp(), null, null, null,
//                            null, null, null, null, null, null);
//
//                    evaluacionEconomica.add(
//                            new EvaluacionEconomica(oportunity.getFechainicioperfexploratorio(),
//                                    null, null, null,
//                                    inversionesExploratoriasAnioAnterior, null, null));
//
//                    var inversionesDesarrolloAnioAnterior = new Inversiones(
//                            invDesarrollo.getDesarrolloSinOperacional(), null, null, null, null,
//                            invDesarrollo.getDesarrolloSinOperacional(), invDesarrollo.getPerforacionDes(),
//                            invDesarrollo.getTerminacionDes(), invDesarrollo.getInfraestructuraDes(), null,
//                            null, null, null, invDesarrollo.getDesarrolloSinOperacional());
//
//                    var anioAnteriorInversionDesarrollo = Integer.parseInt(item.getAnio()) - 1;
//                    evaluacionEconomica
//                            .add(new EvaluacionEconomica(Integer.toString(anioAnteriorInversionDesarrollo),
//                                    null, null, null, inversionesDesarrolloAnioAnterior, null, null));
//                }
                    double pozosTotales = 13.9;
                    assert infoInversion != null;


                    double cantManifolds =  Math.ceil(pozosTotales/6.0);
                    double ductos = 0;
                    double plataformasDesarrollo = 0;
                    for (OportunidadPlanDesarrollo plan : planDesarrollo) {
                        String nombreVersion = plan.getNombreVersion();
                        String prefix = nombreVersion.substring(0, 3); // Obtiene los primeros 3 caracteres
                        char lastChar = nombreVersion.charAt(nombreVersion.length() - 1); // Obtiene el último carácter
                        if (Character.isDigit(lastChar)) { // Verifica si el último carácter es un dígito
                            int lastDigit = Character.getNumericValue(lastChar); // Convierte el carácter a número
                            if (lastDigit >= 2) { // Para versiones 2 o superiores
                                var anioInicioPerfexploratorio = Integer.parseInt(oportunity.getFechainicioperfexploratorio());
                                var anioInicio = Integer.parseInt(oportunity.getFechainicio());
                                if (prefix.equals("Mar")) {
                                    // Caso "Marino 2 o más"
                                    if (anioInicioPerfexploratorio + plan.getDuracion() == anioInicio) {
                                        ductos = infoInversion.getDucto() * paridad.getParidad();
                                        plataformasDesarrollo = infoInversion.getPlataformadesarrollo() * paridad.getParidad();
                                        inversionesAnioAnterior.setDuctos(ductos);
                                        inversionesAnioAnterior.setPlataformaDesarrollo(plataformasDesarrollo);
                                        // Falta añadirlos a la lista de inversiones anio anterior
                                        var risersG = infoInversion.getRisers() * paridad.getParidad() * cantManifolds;
                                        var sistemaDeControlG = infoInversion.getSistemasdecontrol() * paridad.getParidad() * cantManifolds;
                                    }else{
                                        // Falta añadirlos a la lista de inversiones anio actual
                                        var arbolesSubmarinosG = infoInversion.getArbolessubmarinos() * paridad.getParidad() * pozosTotales;
                                        var manifoldsG = infoInversion.getManifolds() * paridad.getParidad() * cantManifolds;
                                    }
                                }
                            } else {
                                ductos = infoInversion.getDucto() * paridad.getParidad();
                                plataformasDesarrollo = infoInversion.getPlataformadesarrollo() * paridad.getParidad();
                                inversionesAnioActual.setDuctos(ductos);
                                inversionesAnioActual.setPlataformaDesarrollo(plataformasDesarrollo);
                            }
                        }
                    }


                    var lineaDescarga = infoInversion.getLineadedescarga() * terminado.doubleValue()
                            * paridad.getParidad();

                    var futuroDesarrollo = costoOperacionMap.get(item.getAnio() + "-19")
                            * produccionDiariaPromedio.get(item.getAnio()).getMbpce() * yearDays
                            * paridad.getParidad() / 1000;

                    inversionesAnioActual.setLineaDescarga(lineaDescarga);
                    inversionesAnioActual.setOperacionalFuturoDesarrollo(futuroDesarrollo);

                } else if (pozosPerforados.containsKey(anioActualInteger)) {
                    perforado = pozosPerforados.get(anioActualInteger);

                    assert terminado != null;
                    var terminadoFinal = terminado;
                    if (anioActualInteger == finalAnioFinal) {
                        terminadoFinal = terminado.subtract(new BigDecimal(1));
                    }

                    assert fiDesarrollo != null;
                    var invDesarrollo = DataProcess.calculaInversionDesarrollo(fiDesarrollo,
                            paridad.getParidad(), terminadoFinal, perforado, aniosPerforacion);
                    var anioInversion = Integer.parseInt(item.getAnio()) - 1;
                    evaluacionEconomica.forEach(element -> {
                        if (element.getAnio().equals(Integer.toString(anioInversion))) {
                            element.getInversiones()
                                    .setDesarrolloSinOperacional(invDesarrollo.getDesarrolloSinOperacional());
                            element.getInversiones().setTerminacionDes(invDesarrollo.getTerminacionDes());
                            element.getInversiones().setPerforacionDes(invDesarrollo.getPerforacionDes());
                            element.getInversiones()
                                    .setInfraestructuraDes(invDesarrollo.getInfraestructuraDes());
                        }
                    });

                    assert infoInversion != null;
                    var lineaDescarga = infoInversion.getLineadedescarga() * terminado.doubleValue()
                            * paridad.getParidad();
                    var futuroDesarrollo = costoOperacionMap.get(item.getAnio() + "-19")
                            * produccionDiariaPromedio.get(item.getAnio()).getMbpce() * yearDays
                            * paridad.getParidad() / 1000;
                    inversionesAnioActual.setLineaDescarga(lineaDescarga);
                    inversionesAnioActual.setOperacionalFuturoDesarrollo(futuroDesarrollo);

                }else if(pozosTerminados.containsKey(anioActualInteger)){

                    terminado = pozosTerminados.get(anioActualInteger);

                    var terminadoFinal = terminado;

                    if (anioActualInteger == finalAnioFinal) {
                        terminadoFinal = terminado.subtract(new BigDecimal(1));
                    }

                    assert fiDesarrollo != null;
                    var invDesarrollo = DataProcess.calculaInversionDesarrollo(fiDesarrollo,
                            paridad.getParidad(), terminadoFinal, perforado, aniosPerforacion);
                    var anioInversion = Integer.parseInt(item.getAnio()) - 1;
                    evaluacionEconomica.forEach(element -> {
                        if (element.getAnio().equals(Integer.toString(anioInversion))) {
                            element.getInversiones()
                                    .setDesarrolloSinOperacional(invDesarrollo.getDesarrolloSinOperacional());
                            element.getInversiones().setTerminacionDes(invDesarrollo.getTerminacionDes());
                            element.getInversiones().setPerforacionDes(invDesarrollo.getPerforacionDes());
                            element.getInversiones()
                                    .setInfraestructuraDes(invDesarrollo.getInfraestructuraDes());
                        }
                    });

                    assert infoInversion != null;
                    var lineaDescarga = infoInversion.getLineadedescarga() * terminado.doubleValue()
                            * paridad.getParidad();
                    var futuroDesarrollo = costoOperacionMap.get(item.getAnio() + "-19")
                            * produccionDiariaPromedio.get(item.getAnio()).getMbpce() * yearDays
                            * paridad.getParidad() / 1000;
                    inversionesAnioActual.setLineaDescarga(lineaDescarga);
                    inversionesAnioActual.setOperacionalFuturoDesarrollo(futuroDesarrollo);

                }





                evaluacionEconomica.add(new EvaluacionEconomica(item.getAnio(),
                        new Pozo(item.getPromedioAnual(), perforado, terminado),
                        produccionDiariaPromedio.get(item.getAnio()), ingresosMap.get(item.getAnio()),
                        inversionesAnioActual, costosMap.get(item.getAnio()), null));

            });

            DataProcess.finalProcessInversiones(evaluacionEconomica);
            log.info("--------------------------------sañdña-------------------------");


            DataProcess.calculaFlujoContable(evaluacionEconomica);

            var flujosContablesTotales = DataProcess.calculaFlujosContablesTotales(evaluacionEconomica,
                    produccionTotalMmbpce, factorInversion, pce);




            return new EvaluacionResponse(oportunity, evaluacionEconomica, flujosContablesTotales, factorCalculo);

        }

        }
    }


