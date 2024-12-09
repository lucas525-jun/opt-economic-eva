package mx.com.innovating.cloud.orchestrator.utilities;

import java.math.BigDecimal;
import java.time.Year;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import lombok.extern.slf4j.Slf4j;
import mx.com.innovating.cloud.data.models.EscaleraProduccion;
import mx.com.innovating.cloud.data.models.FactorInversion;
import mx.com.innovating.cloud.data.models.FactorInversionDesarrollo;
import mx.com.innovating.cloud.data.models.FactorInversionExploratorio;
import mx.com.innovating.cloud.data.models.Paridad;
import mx.com.innovating.cloud.data.models.ProduccionTotalMmbpce;
import mx.com.innovating.cloud.orchestrator.models.*;

@Slf4j
public class DataProcess {

	public static Inversiones calculaInversionExploratoria(FactorInversionExploratorio fiExploratorio, Double paridad) {
		var inversion = new Inversiones();
		var perforacionExploratoria = fiExploratorio.getPerforacion() * paridad;
		var terminacionExploratoria = fiExploratorio.getTerminacion() * paridad;
		var infraestructuraExploratoria = fiExploratorio.getInfraestructura() * paridad;
		var inversionExploratoria = perforacionExploratoria + terminacionExploratoria + infraestructuraExploratoria;

		inversion.setExploratoria(inversionExploratoria);
		inversion.setPerforacionExp(perforacionExploratoria);
		inversion.setTerminacionExp(terminacionExploratoria);
		inversion.setInfraestructuraExp(infraestructuraExploratoria);

		return inversion;
	}

	public static Inversiones calculaInversionDesarrollo(FactorInversionDesarrollo fiDesarrollo, Double paridad,
														 BigDecimal terminado, BigDecimal perforado, Integer aniosPerforacion) {
		var inversion = new Inversiones();

		var perforacionDesarrollo = fiDesarrollo.getPerforacion() * paridad * perforado.doubleValue();


		double terminacionDesarrollo;
		double infraestructuraDesarrollo;

		// Si pozos perforados y terminados estan en un solo año, un año antes se calcula
		//inversion en pozo perforado * nro de pozos perforados * paridad
		//inversion en infraestructura  * nro de pozos perforados * paridad
		//inversion en terminacion * nro de pozos perforados * paridad
		if(aniosPerforacion == 1){
			terminacionDesarrollo = fiDesarrollo.getTerminacion() * paridad * perforado.doubleValue();
			infraestructuraDesarrollo = fiDesarrollo.getInfraestructura() * paridad * perforado.doubleValue();
		}else{
			terminacionDesarrollo = fiDesarrollo.getTerminacion() * paridad * terminado.doubleValue();
			infraestructuraDesarrollo = fiDesarrollo.getInfraestructura() * paridad * terminado.doubleValue();
		}

		// REGLA DE INVERSIONES A FUTURO
		// El calculo de inversion en futuro desarrollo debe realizarse en todos
		// los años donde haya pozos perforados y/o terminados



		var inversionDesarrollo = perforacionDesarrollo + terminacionDesarrollo + infraestructuraDesarrollo;
		inversion.setDesarrolloSinOperacional(inversionDesarrollo);
		inversion.setPerforacionDes(perforacionDesarrollo);
		inversion.setTerminacionDes(terminacionDesarrollo);
		inversion.setInfraestructuraDes(infraestructuraDesarrollo);


		return inversion;
	}

	public static Map<String, Costos> calculaCostosByAnio(Map<String, Double> costoOperacionMap,
														  Map<String, ProduccionDiariaPromedio> pdp, Paridad paridad) {

		Map<String, Costos> costosMap = new HashMap<>();
		pdp.forEach((k, v) -> {

			int yearDays = Year.of(Integer.parseInt(k)).length();

			var operacionInicial = v.getMbpce() * yearDays * paridad.getParidad() / 1000;

			var fijos = (costoOperacionMap.get(k + "-17") == null ? 0 : costoOperacionMap.get(k + "-17"))
					* operacionInicial;

			var administracionCorporativo = (costoOperacionMap.get(k + "-21") == null ? 0
					: costoOperacionMap.get(k + "-21")) * operacionInicial;
			var comprasGas = (costoOperacionMap.get(k + "-24") == null ? 0 : costoOperacionMap.get(k + "-24"))
					* operacionInicial;
			var comprasInterorganismos = (costoOperacionMap.get(k + "-30") == null ? 0
					: costoOperacionMap.get(k + "-30")) * operacionInicial;

			var jubilados = (costoOperacionMap.get(k + "-18") == null ? 0 : costoOperacionMap.get(k + "-18"))
					* operacionInicial;
			var manoObra = (costoOperacionMap.get(k + "-29") == null ? 0 : costoOperacionMap.get(k + "-29"))
					* operacionInicial;

			var materiales = (costoOperacionMap.get(k + "-22") == null ? 0 : costoOperacionMap.get(k + "-22"))
					* operacionInicial;
			var otrosConceptos = (costoOperacionMap.get(k + "-20") == null ? 0 : costoOperacionMap.get(k + "-20"))
					* operacionInicial;
			var reservaLaboral = (costoOperacionMap.get(k + "-26") == null ? 0 : costoOperacionMap.get(k + "-26"))
					* operacionInicial;
			var serviciosCorporativos = (costoOperacionMap.get(k + "-27") == null ? 0
					: costoOperacionMap.get(k + "-27")) * operacionInicial;
			var serviciosGenerales = (costoOperacionMap.get(k + "-16") == null ? 0 : costoOperacionMap.get(k + "-16"))
					* operacionInicial;

			var variables = administracionCorporativo + comprasGas + comprasInterorganismos + jubilados + manoObra
					+ materiales + otrosConceptos + reservaLaboral + serviciosCorporativos + serviciosGenerales;

			var total = variables + fijos;

			var costos = new Costos(total, fijos, variables, administracionCorporativo, comprasGas,
					comprasInterorganismos, jubilados, manoObra, materiales, otrosConceptos, reservaLaboral,
					serviciosCorporativos, serviciosGenerales);

			costosMap.put(k, costos);

		});
		return costosMap;
	}

	public static Map<String, Ingresos> calculaIngresosByAnio(Paridad paridad,
															  Map<String, ProduccionDiariaPromedio> produccionDiariaPromedio,
															  Map<String, Double> preciosMap) {

		Map<String, Ingresos> ingresosMap = new HashMap<>();
		produccionDiariaPromedio.forEach((k, v) -> {

			int yearDays = Year.of(Integer.parseInt(k)).length();
			var operacionConstante = paridad.getParidad() * yearDays / 1000;

			var aceiteExtraPesado = v.getAceiteExtraPesado()
					* (preciosMap.get(k + "-6") == null ? 0 : preciosMap.get(k + "-6")) * operacionConstante;
			var aceitePesado = v.getAceitePesado() * (preciosMap.get(k + "-1") == null ? 0 : preciosMap.get(k + "-1"))
					* operacionConstante;
			var aceiteLigero = v.getAceiteLigero() * (preciosMap.get(k + "-2") == null ? 0 : preciosMap.get(k + "-2"))
					* operacionConstante;
			var aceiteSuperLigero = v.getAceiteSuperLigero()
					* (preciosMap.get(k + "-4") == null ? 0 : preciosMap.get(k + "-4")) * operacionConstante;

			var gasHumedo = v.getGasHumedo() * (preciosMap.get(k + "-5") == null ? 0 : preciosMap.get(k + "-5"))
					* operacionConstante;
			var gasSeco = v.getGasSeco() * (preciosMap.get(k + "-3") == null ? 0 : preciosMap.get(k + "-3"))
					* operacionConstante;

			var condensado = v.getCondensado() * (preciosMap.get(k + "-8") == null ? 0 : preciosMap.get(k + "-8"))
					* operacionConstante;

			var total = aceiteExtraPesado + aceitePesado + aceiteLigero + aceiteSuperLigero + gasHumedo + gasSeco
					+ condensado;

			var ingresos = new Ingresos(total, aceiteExtraPesado, aceitePesado, aceiteLigero, aceiteSuperLigero,
					gasHumedo, gasSeco, condensado);

			ingresosMap.put(k, ingresos);
		});
		return ingresosMap;
	}

	public static Map<String, ProduccionDiariaPromedio> calculaProduccionDiariaPromedioByAnio(
			FactorInversion factorInversion, Map<String, Double> vectorProduccion, Integer idhidrocarburo) {
		Map<String, ProduccionDiariaPromedio> produccionDiariaPromedioMap = new HashMap<>();

		vectorProduccion.forEach((k, v) -> {

			var aceiteExtraPesado = 0.0;
			var aceitePesado = 0.0;
			var aceiteLigero = 0.0;
			var aceiteSuperLigero = 0.0;

			var gasHumedo = 0.0;
			var gasSeco = 0.0;

			var condensado = 0.0;

			switch (idhidrocarburo) {
			case 6, 1, 2, 4 -> {
				aceiteExtraPesado = (idhidrocarburo.equals(6) ? factorInversion.getFactorAceite() : 0) * v;
				aceitePesado = (idhidrocarburo.equals(1) ? factorInversion.getFactorAceite() : 0) * v;
				aceiteLigero = (idhidrocarburo.equals(2) ? factorInversion.getFactorAceite() : 0) * v;
				aceiteSuperLigero = (idhidrocarburo.equals(4) ? factorInversion.getFactorAceite() : 0) * v;
				gasHumedo = v * factorInversion.getFactorGas();
			}
			case 3, 5 -> {
				gasHumedo = (idhidrocarburo.equals(5) ? factorInversion.getFactorGas() : 0) * v;
				gasSeco = (idhidrocarburo.equals(3) ? factorInversion.getFactorGas() : 0) * v;
			}
			}

			condensado = v * factorInversion.getFactorCondensado();

			var resProduccionDiariaPromedio = new ProduccionDiariaPromedio();
			resProduccionDiariaPromedio.setMbpce(v);
			resProduccionDiariaPromedio
					.setAceiteTotal(aceitePesado + aceiteExtraPesado + aceiteLigero + aceiteSuperLigero);
			resProduccionDiariaPromedio.setAceiteExtraPesado(aceiteExtraPesado);
			resProduccionDiariaPromedio.setAceitePesado(aceitePesado);
			resProduccionDiariaPromedio.setAceiteLigero(aceiteLigero);
			resProduccionDiariaPromedio.setAceiteSuperLigero(aceiteSuperLigero);
			resProduccionDiariaPromedio.setGasTotal(gasSeco + gasHumedo);
			resProduccionDiariaPromedio.setGasHumedo(gasHumedo);
			resProduccionDiariaPromedio.setGasSeco(gasSeco);
			resProduccionDiariaPromedio.setCondensado(condensado);

			produccionDiariaPromedioMap.put(k, resProduccionDiariaPromedio);

		});
		return produccionDiariaPromedioMap;
	}

	public static Map<Integer, BigDecimal> getPozosPerforadosByAnio(List<EscaleraProduccion> listTerminados,
			String anioInicio) {

		Map<Integer, BigDecimal> perforados = new HashMap<>();
		var bigDecimal = new BigDecimal(1);

		listTerminados.forEach(item -> {

			if (!item.getAnio().equals(anioInicio)) {
				var anioGuardar = item.getMes() == 1 ? Integer.parseInt(item.getAnio()) - 1
						: Integer.parseInt(item.getAnio());
				perforados.put(anioGuardar,
						perforados.containsKey(anioGuardar) ? perforados.get(anioGuardar).add(bigDecimal) : bigDecimal);
			} else {
				var anioActual = Integer.parseInt(item.getAnio());
				perforados.put(anioActual,
						perforados.containsKey(anioActual) ? perforados.get(anioActual).add(bigDecimal) : bigDecimal);
			}

		});

		return perforados;
	}

	public static Map<Integer, BigDecimal> getPozosterminadosByAnio(List<EscaleraProduccion> listTerminados) {

		Map<Integer, BigDecimal> terminados = new HashMap<>();
		var bigDecimal = new BigDecimal(1);
		listTerminados.forEach(item -> {
			var anioActual = Integer.parseInt(item.getAnio());
			terminados.put(anioActual,
					terminados.containsKey(anioActual) ? terminados.get(anioActual).add(bigDecimal) : bigDecimal);
		});
		return terminados;

	}

	public static void finalProcessInversiones(List<EvaluacionEconomica> evaluacionEconomica) {

		evaluacionEconomica.forEach(eval -> {
			//if (eval.getInversiones().getTotal() == null) {

				var exploratoriaTotal =
						(eval.getInversiones().getPerforacionExp() == null ? 0 : eval.getInversiones().getPerforacionExp())
						+ (eval.getInversiones().getTerminacionExp() == null ? 0 : eval.getInversiones().getTerminacionExp())
						+ (eval.getInversiones().getInfraestructuraExp() == null ? 0 : eval.getInversiones().getInfraestructuraExp());


				//no aplica,  como 0
				var desarrolloSinOperacional =
						(eval.getInversiones().getPerforacionDes() == null ? 0 : eval.getInversiones().getPerforacionDes())
						+ (eval.getInversiones().getTerminacionDes() == null ? 0 : eval.getInversiones().getTerminacionDes())
						+ (eval.getInversiones().getInfraestructuraDes() == null ? 0 : eval.getInversiones().getInfraestructuraDes())
						+ (eval.getInversiones().getLineaDescarga() == null ? 0 : eval.getInversiones().getLineaDescarga())
						+ (eval.getInversiones().getDuctos() == null ? 0 : eval.getInversiones().getDuctos())
						+ (eval.getInversiones().getPlataformaDesarrollo() == null ? 0 : eval.getInversiones().getPlataformaDesarrollo());

				eval.getInversiones().setDesarrolloSinOperacional(desarrolloSinOperacional);


				// no aplica, tiene como 0
				var desarrollo =
						desarrolloSinOperacional
						+ (eval.getInversiones().getOperacionalFuturoDesarrollo() == null ? 0 : eval.getInversiones().getOperacionalFuturoDesarrollo());

				eval.getInversiones().setDesarrollo(desarrollo);
				eval.getInversiones().setTotal(desarrollo + exploratoriaTotal);

			//}
		});

	}

	public static void calculaFlujoContable(List<EvaluacionEconomica> evaluacionEconomica) {

		AtomicInteger flujo = new AtomicInteger();
		evaluacionEconomica.forEach(eval -> {

			var totalInversiones = (eval.getInversiones().getTotal() != null)
					? eval.getInversiones().getTotal()
					: eval.getInversiones().getExploratoria();

			var totalCostos = (eval.getCostos() != null && eval.getCostos().getTotal() != null)
					? eval.getCostos().getTotal()
					: 0.0;

			var totalIngresos = (eval.getIngresos() != null && eval.getIngresos().getTotal() != null)
					? eval.getIngresos().getTotal()
					: 0.0;

			var egresosTotales = totalInversiones + totalCostos;

			var flujosNetosEfectivo = totalIngresos - egresosTotales;

			var flujosDescontadosEfectivo = flujosNetosEfectivo / Math.pow((1 + 0.10), flujo.get());

			var flujosDescontadosInversion = totalInversiones / Math.pow((1 + 0.10), flujo.get());

			var flujosDescontadosEgresos = egresosTotales / Math.pow((1 + 0.10), flujo.get());

			var flujosDescontadosIngresos = totalIngresos / Math.pow((1 + 0.10), flujo.get());

			var flujosDescontadosCostos = totalCostos / Math.pow((1 + 0.10), flujo.get());

			flujo.getAndIncrement();

			var flujoContable = new FlujoContable(egresosTotales, flujosNetosEfectivo, flujosDescontadosEfectivo,
					flujosDescontadosInversion, flujosDescontadosEgresos, flujosDescontadosIngresos,
					flujosDescontadosCostos);

			eval.setFlujoContable(flujoContable);

		});

	}

	public static FlujoContableTotal calculaFlujosContablesTotales(List<EvaluacionEconomica> evaluacionEconomica,
			ProduccionTotalMmbpce produccionTotalMmbpce, FactorInversion factorInversion, double pce) {

		List<Double> flujosNetosEfectivo = new ArrayList<>();
		List<Double> flujosDescontadosEfectivoList = new ArrayList<>();
		List<Double> valorPresenteInversionList = new ArrayList<>();
		List<Double> valorPresenteEgresosList = new ArrayList<>();
		List<Double> valorPresenteIngresosList = new ArrayList<>();
		List<Double> valorPresenteCostosList = new ArrayList<>();

		var inversionExploratoria = evaluacionEconomica.get(0).getInversiones().getExploratoria();
		var desarrolloSinOperacional = evaluacionEconomica.get(0).getInversiones().getDesarrolloSinOperacional();
		List<Double> listTotalesInversiones = new ArrayList<>();

		var perfDes = evaluacionEconomica.get(0).getInversiones().getPerforacionDes();


		List<Double> listTotalesCostos = new ArrayList<>();

		evaluacionEconomica.forEach(eval -> {
			flujosNetosEfectivo.add(eval.getFlujoContable().getFlujosNetosEfectivo());
			flujosDescontadosEfectivoList.add(eval.getFlujoContable().getFlujosDescontadosEfectivo());
			valorPresenteInversionList.add(eval.getFlujoContable().getFlujosDescontadosInversion());
			valorPresenteEgresosList.add(eval.getFlujoContable().getFlujosDescontadosEgresos());
			valorPresenteIngresosList.add(eval.getFlujoContable().getFlujosDescontadosIngresos());
			valorPresenteCostosList.add(eval.getFlujoContable().getFlujosDescontadosCostos());
			listTotalesInversiones.add(eval.getInversiones().getTotal());
			listTotalesCostos.add(eval.getCostos() == null ? 0 : eval.getCostos().getTotal());
		});

		listTotalesInversiones.remove(0);

		var totalInversiones = 0.0;
		for (Double total : listTotalesInversiones) {
			totalInversiones += total == null ? 0 : total;
		}

		totalInversiones += desarrolloSinOperacional == null ? 0 : desarrolloSinOperacional;

		log.info("::::: totalInversiones {}", totalInversiones);

		var costosTotal = 0.0;
		for (double totalCosto : listTotalesCostos) {
			costosTotal += totalCosto;
		}
		var flujosDescontadosEfectivoTotal = 0.0;
		for (double fdet : flujosDescontadosEfectivoList) {
			flujosDescontadosEfectivoTotal += fdet;
		}
		var valorPresenteInversion = 0.0;
		for (double vpi : valorPresenteInversionList) {
			valorPresenteInversion += vpi;
		}
		var valorPresenteEgresos = 0.0;
		for (double vpe : valorPresenteEgresosList) {
			valorPresenteEgresos += vpe;
		}
		var valorPresenteIngresos = 0.0;
		for (double vpIngresos : valorPresenteIngresosList) {
			valorPresenteIngresos += vpIngresos;
		}
		var valorPresenteCostos = 0.0;
		for (double vpc : valorPresenteCostosList) {
			valorPresenteCostos += vpc;
		}

		var inversionInicial = flujosNetosEfectivo.get(0);

		log.info("::::: inversionInicial {}", inversionInicial);

		flujosNetosEfectivo.remove(0);

		var vpn = calculaVpn(inversionInicial, flujosNetosEfectivo, 0.10);

		log.info("::::: VPN calculo final {}", vpn);


		Double tir = 0.0;
		var reporte708 = 0.0;
		var reporte721 = 0.0;
		var reporte722 = 0.0;
		var reporte723 = 0.0;
		var utilidadBpce = 0.0;
		var relacionCostoBeneficio = 0.0;
		var costoDescubrimiento720 = 0.0;
		var costoOperacion = 0.0;

		if (perfDes != 0.0) {
			 tir = calculaTir(vpn, inversionInicial, flujosNetosEfectivo, 0.10);


		} else {

			 tir = 0.0;


		}



		log.info("::::: TIR calculo final {}", tir);


		var reporte120 = produccionTotalMmbpce.getProduccionTotalMmbpce() * factorInversion.getFactorAceite();
		log.info("::::: reporte120 - {} ", reporte120);
		var reporte121 = produccionTotalMmbpce.getProduccionTotalMmbpce() * factorInversion.getFactorGas();
		log.info("::::: reporte121 - {} ", reporte121);
		var reporte123 = produccionTotalMmbpce.getProduccionTotalMmbpce();
		log.info("::::: reporte123 - {} ", reporte123);



		if (perfDes != 0.0) {

			reporte708 = vpn / valorPresenteInversion;
			reporte721 = totalInversiones / reporte120;
			reporte722 = totalInversiones / reporte121;
			reporte723 = totalInversiones / reporte123;
			utilidadBpce = vpn / reporte123;
			relacionCostoBeneficio = valorPresenteIngresos / valorPresenteEgresos;
			costoOperacion = costosTotal / reporte123;
			costoDescubrimiento720 = inversionExploratoria / pce;


		}



		return new FlujoContableTotal(vpn, tir, flujosDescontadosEfectivoTotal, valorPresenteInversion,
				valorPresenteEgresos, valorPresenteIngresos, valorPresenteCostos, reporte708, reporte721, reporte722,
				reporte723, utilidadBpce, relacionCostoBeneficio, costoDescubrimiento720, costoOperacion);

	}

	private static Double calculaVpn(Double inversionInicial, List<Double> flujo, Double taza) {
		double calc = 0.0;
		for (int i = 0; i < flujo.size(); i++) {
			log.info("::::: flujo {} - {}", i, flujo.get(i));

			calc += flujo.get(i) / Math.pow((1 + taza), (i + 1));

		}

        return calc + inversionInicial;
	}

	private static Double calculaTir(Double vpn, Double inversionInicial, List<Double> flujo, Double taza) {
		Double tir = vpn;
		double tazaUlt = 0.0;
		double tolerancia = 0.01; // Ajusta la tolerancia para el cálculo de TIR
		int maxIteraciones = 10000; // Número máximo de iteraciones
		int iteracion = 0;

		if (vpn > 0) {
			log.info("::::: Calculando TIR con VPN positiva");
			for (double x = taza; Math.abs(tir) > tolerancia && iteracion < maxIteraciones; x += 0.0001) {
				Double calc = 0.0;
				for (int j = 0; j < flujo.size(); j++) {
					calc += flujo.get(j) / Math.pow((1 + x), (j + 1));
				}
				tir = inversionInicial + calc;
				tazaUlt = x;
				iteracion++;
			}
		} else {
			log.info("::::: Calculando TIR con VPN negativa");
			for (double x = taza; Math.abs(tir) > tolerancia && iteracion < maxIteraciones; x -= 0.0001) {
				Double calc = 0.0;
				for (int j = 0; j < flujo.size(); j++) {
					calc += flujo.get(j) / Math.pow((1 + x), (j + 1));
				}
				tir = inversionInicial + calc;
				tazaUlt = x;
				iteracion++;
			}
		}

		if (iteracion >= maxIteraciones) {
			log.warn("Número máximo de iteraciones alcanzado sin converger a un valor de TIR válido.");
		}

		return tazaUlt * 100; // Convertir a porcentaje
	}
}

