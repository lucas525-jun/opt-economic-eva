package mx.com.innovating.cloud.services;

import java.math.BigDecimal;
import java.time.Year;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.eclipse.microprofile.rest.client.inject.RestClient;

import io.smallrye.mutiny.Uni;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import lombok.extern.slf4j.Slf4j;
import mx.com.innovating.cloud.clients.DatabaseConnectorClient;
import mx.com.innovating.cloud.models.CostoOperacion;
import mx.com.innovating.cloud.models.Costos;
import mx.com.innovating.cloud.models.EscaleraProduccion;
import mx.com.innovating.cloud.models.EvaluacionEconomica;
import mx.com.innovating.cloud.models.EvaluacionResponse;
import mx.com.innovating.cloud.models.FactorInversion;
import mx.com.innovating.cloud.models.FactorInversionDesarrollo;
import mx.com.innovating.cloud.models.FactorInversionExploratorio;
import mx.com.innovating.cloud.models.InformacionInversion;
import mx.com.innovating.cloud.models.InformacionOportunidad;
import mx.com.innovating.cloud.models.Ingresos;
import mx.com.innovating.cloud.models.Inversiones;
import mx.com.innovating.cloud.models.Paridad;
import mx.com.innovating.cloud.models.Pozo;
import mx.com.innovating.cloud.models.PozosActivos;
import mx.com.innovating.cloud.models.PrecioHidrocarburo;
import mx.com.innovating.cloud.models.ProduccionDiariaPromedio;
import mx.com.innovating.cloud.models.ProduccionTotalMmbpce;
import mx.com.innovating.cloud.models.VectorProduccion;
import mx.com.innovating.cloud.utilities.DataProcess;

@Slf4j
@ApplicationScoped
public class EvaluacionEconomicaService {

	@Inject
	@RestClient
	DatabaseConnectorClient databaseConnectorClient;

	public Uni<InformacionOportunidad> getInfoOportunidad(Integer idOportunidad) {
		return databaseConnectorClient.getInfoOportunidad(idOportunidad);
	}

	public Uni<EvaluacionResponse> getInfoPozosService(Integer idOportunidad, InformacionOportunidad oportunity) {

		return Uni.combine().all()
				.unis(databaseConnectorClient.getPozosActivos(idOportunidad),
						databaseConnectorClient.getPozosTerminados(idOportunidad),
						databaseConnectorClient.getVectorProduccion(idOportunidad),
						databaseConnectorClient.getFactorInversion(idOportunidad),
						databaseConnectorClient.getPrecioHidrocarburo(idOportunidad, oportunity.getIdprograma()),
						databaseConnectorClient.getFactorInversionExploratorio(idOportunidad),
						databaseConnectorClient.getFactorInversionDesarrollo(idOportunidad),
						databaseConnectorClient.getInformacionInversion(idOportunidad),
						databaseConnectorClient.getCostoOperacion(oportunity.getIdproyecto()),
						databaseConnectorClient.getProduccionTotalMmbpce(idOportunidad),
						databaseConnectorClient.getParidad(oportunity.getFechainicioperfexploratorio()))
				.with(unisList -> {

					FactorInversionExploratorio fiExploratorio;
					if (unisList.get(5) instanceof FactorInversionExploratorio) {
						fiExploratorio = (FactorInversionExploratorio) unisList.get(5);
					} else {
						fiExploratorio = null;
					}

					InformacionInversion infoInversion;
					if (unisList.get(7) instanceof InformacionInversion) {
						infoInversion = (InformacionInversion) unisList.get(7);
					} else {
						infoInversion = null;
					}

					FactorInversionDesarrollo fiDesarrollo;
					if (unisList.get(6) instanceof FactorInversionDesarrollo) {
						fiDesarrollo = (FactorInversionDesarrollo) unisList.get(6);
					} else {
						fiDesarrollo = null;
					}

					ProduccionTotalMmbpce produccionTotalMmbpce;
					if (unisList.get(9) instanceof ProduccionTotalMmbpce) {
						produccionTotalMmbpce = (ProduccionTotalMmbpce) unisList.get(9);
					} else {
						produccionTotalMmbpce = null;
					}

					Paridad paridad;
					if (unisList.get(10) instanceof Paridad) {
						paridad = (Paridad) unisList.get(10);
					} else {
						paridad = null;
					}

					FactorInversion factorInversion;
					if (unisList.get(3) instanceof FactorInversion) {
						factorInversion = (FactorInversion) unisList.get(3);
					} else {
						factorInversion = null;
					}

					List<PozosActivos> listActivos = null;
					if (((List<?>) unisList.get(0)).get(0) instanceof PozosActivos) {
						listActivos = (List<PozosActivos>) unisList.get(0);
					}

					List<EscaleraProduccion> listTerminados = null;
					if (((List<?>) unisList.get(1)).get(0) instanceof EscaleraProduccion) {
						listTerminados = (List<EscaleraProduccion>) unisList.get(1);
					}

					List<VectorProduccion> listVectorProduccion = null;
					if (((List<?>) unisList.get(2)).get(0) instanceof VectorProduccion) {
						listVectorProduccion = (List<VectorProduccion>) unisList.get(2);
					}

					List<PrecioHidrocarburo> listPrecios = null;
					if (((List<?>) unisList.get(4)).get(0) instanceof PrecioHidrocarburo) {
						listPrecios = (List<PrecioHidrocarburo>) unisList.get(4);
					}

					List<CostoOperacion> listCostoOperacion = null;
					if (((List<?>) unisList.get(8)).get(0) instanceof CostoOperacion) {
						listCostoOperacion = (List<CostoOperacion>) unisList.get(8);
					}

					int yearDays = Year.of(Integer.parseInt(oportunity.getFechainicioperfexploratorio())).length();
					//int yearDays = 365;

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
							produccionDiariaPromedio, preciosMap, yearDays);

					Map<String, Double> costoOperacionMap = new HashMap<>();
					assert listCostoOperacion != null;
					listCostoOperacion.forEach(element -> costoOperacionMap
							.put(element.getAnio() + "-" + element.getIdcostooperacion(), element.getGasto()));

					Map<String, Costos> costosMap = DataProcess.calculaCostosByAnio(costoOperacionMap,
							produccionDiariaPromedio, yearDays, paridad);

					log.info("Generando Respuesta");
					List<EvaluacionEconomica> evaluacionEconomica = new ArrayList<>();
					int finalAnioFinal = anioFinal;
					assert listActivos != null;
					listActivos.forEach(item -> {
						var anioActualInteger = Integer.parseInt(item.getAnio());
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

				});

	}

}
