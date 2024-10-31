package mx.com.innovating.cloud.orchestrator.services;

import java.math.BigDecimal;
import java.time.Year;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import io.quarkus.logging.Log;
import mx.com.innovating.cloud.data.entities.InformacionOportunidad;
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
import org.eclipse.microprofile.rest.client.inject.RestClient;

import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import lombok.extern.slf4j.Slf4j;
import mx.com.innovating.cloud.data.models.*;
import mx.com.innovating.cloud.data.entities.*;
import mx.com.innovating.cloud.orchestrator.utilities.DataProcess;

@Slf4j
@ApplicationScoped
public class EvaluacionEconomicaService {

    @Inject
    DataBaseConnectorRepository databaseConnectorClient;

    public EvaluacionResponse getInfoPozosService(Integer idOportunidadObjetivo, Integer version) {
        Log.info("  1 / 12 - getPozoPerforados");
        InformacionOportunidad oportunity = databaseConnectorClient.getInfoOportunidad(idOportunidadObjetivo);

        if (oportunity == null) {
            Log.error("InformacionOportunidad is null");
            throw new RuntimeException("InformacionOportunidad is null");
        }


        Log.info("  2 / 12 - getPozoPerforados");
        List<EscaleraProduccion> listTerminados = databaseConnectorClient.getPozosPerforados(idOportunidadObjetivo, version); //getPozosTerminados(idOportunidad),

        Log.info("  3 / 12 - getFactorInversionExploratorio");
        FactorInversionExploratorio fiExploratorio = databaseConnectorClient.getFactorInversionExploratorio(idOportunidadObjetivo);

        Log.info("  4 / 12 - getFactorInversionDesarrollo");
        FactorInversionDesarrollo fiDesarrollo = databaseConnectorClient.getFactorInversionDesarrollo(idOportunidadObjetivo);

        Log.info("  5 / 12 - getInformacionInversion");
        InformacionInversion infoInversion = databaseConnectorClient.getInformacionInversion(idOportunidadObjetivo);

        Log.info("  6 / 12 - getCostoOperacion");
        List<CostoOperacion> listCostoOperacion = databaseConnectorClient.getCostoOperacion(oportunity.getIdproyecto());

        Log.info("  7 / 12 - getProduccionTotalMmbpce");
        ProduccionTotalMmbpce produccionTotalMmbpce = databaseConnectorClient.getProduccionTotalMmbpce(idOportunidadObjetivo, version);

        Log.info("  8 / 12 - getParidad");
        Paridad paridad = databaseConnectorClient.getParidad(Integer.valueOf(oportunity.getFechainicioperfexploratorio()));

        Log.info("  9 / 12 - getVectorProduccion");
        List<VectorProduccion> listVectorProduccion = databaseConnectorClient.getVectorProduccion(idOportunidadObjetivo, version);

        Log.info(" 10 / 12 - getPrecioHidrocarburo");
        List<PrecioHidrocarburo> listPrecios = databaseConnectorClient.getPrecioHidrocarburo(idOportunidadObjetivo, oportunity.getIdprograma());


        Log.info(" 11 / 12 - getPozosActivos");
        List<PozosActivos> listActivos = databaseConnectorClient.getPozosActivos(idOportunidadObjetivo, version);

        // error
        Log.warn("Consultas que no funcionan");
        Log.info(" 12 / 12 - getFactorInversion");
        FactorInversion factorInversion = databaseConnectorClient.getFactorInversion(idOportunidadObjetivo);


        log.info("Obteniendo la informacion de los pozos perforados");
        assert listTerminados != null;
        Map<Integer, BigDecimal> pozosPerforados = DataProcess.getPozosPerforadosByAnio(listTerminados,
                oportunity.getFechainicio());

        log.info("::::: numero de anios con pozos perforados: {}", pozosPerforados.size());

        log.info("Obteniendo la informacion de los pozos terminados");
        Map<Integer, BigDecimal> pozosTerminados = DataProcess.getPozosterminadosByAnio(listTerminados);

        log.info("::::: numero de anios con pozos terminados: {}", pozosTerminados.size());

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
        listVectorProduccion.forEach(item -> vectorProduccion.put(item.getAnio(), item.getTotalanual()));

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
            BigDecimal perforado = null;
            BigDecimal terminado = null;

            var inversionesAnioActual = new Inversiones();
            if (pozosTerminados.containsKey(anioActualInteger)) {
                terminado = pozosTerminados.get(anioActualInteger);
            }
            if (pozosPerforados.containsKey(anioActualInteger)
                    && item.getAnio().equals(oportunity.getFechainicio())) {
                perforado = pozosPerforados.get(anioActualInteger).subtract(new BigDecimal(1));

                assert fiExploratorio != null;
                var invExploratoria = DataProcess.calculaInversionExploratoria(fiExploratorio,
                        paridad.getParidad());

                assert terminado != null;
                assert fiDesarrollo != null;
                var invDesarrollo = DataProcess.calculaInversionDesarrollo(fiDesarrollo,
                        paridad.getParidad(), terminado, perforado);

                if (finalAnioFinal == anioActualInteger) {
                    var totalInversionAnioAnterior = invDesarrollo.getDesarrolloSinOperacional()
                            + invExploratoria.getExploratoria();
                    var inversionesAnioAnterior = new Inversiones(totalInversionAnioAnterior,
                            invExploratoria.getExploratoria(), invExploratoria.getPerforacionExp(),
                            invExploratoria.getTerminacionExp(), invExploratoria.getInfraestructuraExp(),
                            invDesarrollo.getDesarrolloSinOperacional(), invDesarrollo.getPerforacionDes(),
                            invDesarrollo.getTerminacionDes(), invDesarrollo.getInfraestructuraDes(), null,
                            null, null, null, invDesarrollo.getDesarrolloSinOperacional());

                    var anioAnteriorInversion = Integer.parseInt(item.getAnio()) - 1;
                    evaluacionEconomica.add(new EvaluacionEconomica(Integer.toString(anioAnteriorInversion),
                            null, null, null, inversionesAnioAnterior, null, null));
                } else {
                    var inversionesExploratoriasAnioAnterior = new Inversiones(
                            invExploratoria.getExploratoria(), invExploratoria.getExploratoria(),
                            invExploratoria.getPerforacionExp(), invExploratoria.getTerminacionExp(),
                            invExploratoria.getInfraestructuraExp(), null, null, null, null, null, null,
                            null, null, null);

                    var anioAnteriorInversionExploratoria = Integer.parseInt(item.getAnio()) - 2;
                    evaluacionEconomica.add(
                            new EvaluacionEconomica(Integer.toString(anioAnteriorInversionExploratoria),
                                    null, null, null, inversionesExploratoriasAnioAnterior, null, null));

                    var inversionesDesarrolloAnioAnterior = new Inversiones(
                            invDesarrollo.getDesarrolloSinOperacional(), null, null, null, null,
                            invDesarrollo.getDesarrolloSinOperacional(), invDesarrollo.getPerforacionDes(),
                            invDesarrollo.getTerminacionDes(), invDesarrollo.getInfraestructuraDes(), null,
                            null, null, null, invDesarrollo.getDesarrolloSinOperacional());

                    var anioAnteriorInversionDesarrollo = Integer.parseInt(item.getAnio()) - 1;
                    evaluacionEconomica
                            .add(new EvaluacionEconomica(Integer.toString(anioAnteriorInversionDesarrollo),
                                    null, null, null, inversionesDesarrolloAnioAnterior, null, null));
                }

                assert infoInversion != null;
                var lineaDescarga = infoInversion.getLineadedescarga() * terminado.doubleValue()
                        * paridad.getParidad();
                var ductos = infoInversion.getDucto() * paridad.getParidad();
                var plataformasDesarrollo = infoInversion.getPlataformadesarrollo() * paridad.getParidad();

                var futuroDesarrollo = costoOperacionMap.get(item.getAnio() + "-19")
                        * produccionDiariaPromedio.get(item.getAnio()).getMbpce() * yearDays
                        * paridad.getParidad() / 1000;

                inversionesAnioActual.setLineaDescarga(lineaDescarga);
                inversionesAnioActual.setDuctos(ductos);
                inversionesAnioActual.setPlataformaDesarrollo(plataformasDesarrollo);
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
                        paridad.getParidad(), terminadoFinal, perforado);
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

        DataProcess.calculaFlujoContable(evaluacionEconomica);

        var flujosContablesTotales = DataProcess.calculaFlujosContablesTotales(evaluacionEconomica,
                produccionTotalMmbpce, factorInversion);

        return new EvaluacionResponse(oportunity, evaluacionEconomica, flujosContablesTotales);

    }

}
