package mx.com.innovating.cloud.orchestrator.services;

import jakarta.enterprise.context.ApplicationScoped;
import mx.com.innovating.cloud.data.entities.InformacionOportunidad;
import mx.com.innovating.cloud.orchestrator.models.*;
import mx.com.innovating.cloud.orchestrator.utilities.DataProcess;

import java.math.BigDecimal;
import java.util.*;
import java.util.stream.Collectors;

@ApplicationScoped
public class EvaluacionMerger {

    public EvaluacionResponse mergeEvaluaciones(List<EvaluacionResponse> responses,
            double[] produccionTotalMmbpceArray, double sumPCE, double maxPCE,
            FactorCalculoForMulti factorCalculoForMulti) {

        double totalAreaMasignacion = responses.stream()
                .mapToDouble(EvaluacionResponse::getAreamasignacion)
                .sum();

        // Get last response's infoOportunidad
        InformacionOportunidad firstInfoOportunidad = responses.get(0).getInfoOportunidad();

        Map<String, List<EvaluacionEconomica>> evaluacionesByYear = responses.stream()
                .flatMap(response -> response.getEvaluacionEconomica().stream())
                .collect(Collectors.groupingBy(EvaluacionEconomica::getAnio));
        List<EvaluacionEconomica> mergedEvaluaciones = evaluacionesByYear.entrySet().stream()
                .map(entry -> mergeEvaluacionesForYear(entry.getKey(), entry.getValue()))
                .sorted(Comparator.comparing(EvaluacionEconomica::getAnio))
                .collect(Collectors.toList());

        DataProcess.finalProcessInversiones(mergedEvaluaciones);
        DataProcess.calculaFlujoContable(mergedEvaluaciones);

        // Merge FactorCalculo
        FactorCalculo mergedFactorCalculo = responses.stream()
                .map(EvaluacionResponse::getFactorCalculo)
                .filter(Objects::nonNull)
                .reduce(new FactorCalculo(), (acc, fc) -> {
                    if (acc.getFc_aceite() == null)
                        acc.setFc_aceite(0.0);
                    if (acc.getFc_gas() == null)
                        acc.setFc_gas(0.0);
                    if (acc.getFc_condensado() == null)
                        acc.setFc_condensado(0.0);

                    acc.setFc_aceite(acc.getFc_aceite() + (fc.getFc_aceite() != null ? fc.getFc_aceite() : 0.0));
                    acc.setFc_gas(acc.getFc_gas() + (fc.getFc_gas() != null ? fc.getFc_gas() : 0.0));
                    acc.setFc_condensado(
                            acc.getFc_condensado() + (fc.getFc_condensado() != null ? fc.getFc_condensado() : 0.0));

                    return acc;
                });

        FactorCalculo firstFactorCalculo = responses.get(0).getFactorCalculo();
        if (firstFactorCalculo != null) {
            mergedFactorCalculo.setIdoportunidadobjetivo(firstFactorCalculo.getIdoportunidadobjetivo());
            mergedFactorCalculo.setIdversion(firstFactorCalculo.getIdversion());
        }

        FlujoContableTotal mergedFlujoContableTotal = DataProcess.calculaFlujosContablesTotalesForMulti(
                mergedEvaluaciones, produccionTotalMmbpceArray, factorCalculoForMulti,
                sumPCE);

        String lastFechaTermino = responses.get(responses.size() - 1).getFechaTermino();
        int maxPozosTerminados = responses.stream()
                .mapToInt(EvaluacionResponse::getPozosTerminadosValue)
                .max()
                .orElse(0);

        return new EvaluacionResponse(
                totalAreaMasignacion,
                sumPCE,
                firstInfoOportunidad,
                mergedEvaluaciones,
                mergedFlujoContableTotal,
                mergedFactorCalculo,
                lastFechaTermino,
                maxPozosTerminados, factorCalculoForMulti);
    }

    private EvaluacionEconomica mergeEvaluacionesForYear(String year, List<EvaluacionEconomica> evaluaciones) {
        // Use a new EvaluacionEconomica object initialized to zero
        EvaluacionEconomica merged = new EvaluacionEconomica(year, new Pozo(), new ProduccionDiariaPromedio(),
                new Ingresos(), new Inversiones(), new Costos(), new FlujoContable());

        for (EvaluacionEconomica current : evaluaciones) {
            merged.setPozosProduccion(mergePozos(merged.getPozosProduccion(), current.getPozosProduccion()));
            merged.setProduccionDiariaPromedio(
                    mergeProduccionDiaria(merged.getProduccionDiariaPromedio(), current.getProduccionDiariaPromedio()));
            merged.setIngresos(mergeIngresos(merged.getIngresos(), current.getIngresos()));
            merged.setInversiones(mergeInversiones(merged.getInversiones(), current.getInversiones()));
            merged.setCostos(mergeCostos(merged.getCostos(), current.getCostos()));
            merged.setFlujoContable(mergeFlujos(merged.getFlujoContable(), current.getFlujoContable()));
        }

        return merged;
    }

    private Pozo mergePozos(Pozo base, Pozo other) {
        if (base == null)
            return other;
        if (other == null)
            return base;

        return new Pozo(
                // other.getPozosActivos(),
                add(base.getPozosActivos(), other.getPozosActivos()),
                add(base.getPozosPerforados(), other.getPozosPerforados()),
                add(base.getPozosTerminados(), other.getPozosTerminados()));
    }

    public ProduccionDiariaPromedio mergeProduccionDiaria(ProduccionDiariaPromedio base,
            ProduccionDiariaPromedio other) {
        if (base == null)
            return other;
        if (other == null)
            return base;

        return new ProduccionDiariaPromedio(
                sum(base.getMbpce(), other.getMbpce()),
                sum(base.getAceiteTotal(), other.getAceiteTotal()),
                sum(base.getAceiteExtraPesado(), other.getAceiteExtraPesado()),
                sum(base.getAceitePesado(), other.getAceitePesado()),
                sum(base.getAceiteLigero(), other.getAceiteLigero()),
                sum(base.getAceiteSuperLigero(), other.getAceiteSuperLigero()),
                sum(base.getGasTotal(), other.getGasTotal()),
                sum(base.getGasHumedo(), other.getGasHumedo()),
                sum(base.getGasSeco(), other.getGasSeco()),
                sum(base.getCondensado(), other.getCondensado()));
    }

    private Ingresos mergeIngresos(Ingresos base, Ingresos other) {
        if (base == null)
            return other;
        if (other == null)
            return base;

        return new Ingresos(
                sum(base.getTotal(), other.getTotal()),
                sum(base.getAceiteExtraPesado(), other.getAceiteExtraPesado()),
                sum(base.getAceitePesado(), other.getAceitePesado()),
                sum(base.getAceiteLigero(), other.getAceiteLigero()),
                sum(base.getAceiteSuperLigero(), other.getAceiteSuperLigero()),
                sum(base.getGasHumedo(), other.getGasHumedo()),
                sum(base.getGasSeco(), other.getGasSeco()),
                sum(base.getCondensado(), other.getCondensado()));
    }

    private Inversiones mergeInversiones(Inversiones base, Inversiones other) {
        if (base == null)
            return other;
        if (other == null)
            return base;

        return new Inversiones(
                sum(base.getTotal(), other.getTotal()),
                sum(base.getExploratoria(), other.getExploratoria()),
                sum(base.getPerforacionExp(), other.getPerforacionExp()),
                sum(base.getTerminacionExp(), other.getTerminacionExp()),
                sum(base.getInfraestructuraExp(), other.getInfraestructuraExp()),
                sum(base.getDesarrolloSinOperacional(), other.getDesarrolloSinOperacional()),
                sum(base.getPerforacionDes(), other.getPerforacionDes()),
                sum(base.getTerminacionDes(), other.getTerminacionDes()),
                sum(base.getInfraestructuraDes(), other.getInfraestructuraDes()),
                sum(base.getLineaDescarga(), other.getLineaDescarga()),
                sum(base.getDuctos(), other.getDuctos()),
                sum(base.getPlataformaDesarrollo(), other.getPlataformaDesarrollo()),
                sum(base.getSistemaDeControl(), other.getSistemaDeControl()),
                sum(base.getRisers(), other.getRisers()),
                sum(base.getManifolds(), other.getManifolds()),
                sum(base.getArbolSubmarinos(), other.getArbolSubmarinos()),
                sum(base.getEstacionCompresion(), other.getEstacionCompresion()),
                sum(base.getBateria(), other.getBateria()),
                sum(base.getCubiertaProcesos(), other.getCubiertaProcesos()),
                sum(base.getBuqueTanqueCompra(), other.getBuqueTanqueCompra()),
                sum(base.getBuqueTanqueRenta(), other.getBuqueTanqueRenta()),
                sum(base.getOperacionalFuturoDesarrollo(), other.getOperacionalFuturoDesarrollo()),
                sum(base.getMantenimientoDePozos(), other.getMantenimientoDePozos()),
                sum(base.getMantenimientoInfraestructuraFuturoDesarrollo(),
                        other.getMantenimientoInfraestructuraFuturoDesarrollo()),
                sum(base.getDesarrollo(), other.getDesarrollo()));
    }

    private Costos mergeCostos(Costos base, Costos other) {
        if (base == null && other == null)
            return null;

        if (base == null)
            return other;
        if (other == null)
            return base;

        return new Costos(
                sum(base.getTotal(), other.getTotal()),
                sum(base.getFijos(), other.getFijos()),
                sum(base.getVariables(), other.getVariables()),
                sum(base.getAdministracionCorporativo(), other.getAdministracionCorporativo()),
                sum(base.getComprasGas(), other.getComprasGas()),
                sum(base.getComprasInterorganismos(), other.getComprasInterorganismos()),
                sum(base.getJubilados(), other.getJubilados()),
                sum(base.getManoObra(), other.getManoObra()),
                sum(base.getMateriales(), other.getMateriales()),
                sum(base.getOtrosConceptos(), other.getOtrosConceptos()),
                sum(base.getReservaLaboral(), other.getReservaLaboral()),
                sum(base.getServiciosCorporativos(), other.getServiciosCorporativos()),
                sum(base.getServiciosGenerales(), other.getServiciosGenerales()));
    }

    private FlujoContable mergeFlujos(FlujoContable base, FlujoContable other) {
        if (base == null)
            return other;
        if (other == null)
            return base;

        return new FlujoContable(
                sum(base.getEgresosTotales(), other.getEgresosTotales()),
                sum(base.getFlujosNetosEfectivo(), other.getFlujosNetosEfectivo()),
                sum(base.getFlujosDescontadosEfectivo(), other.getFlujosDescontadosEfectivo()),
                sum(base.getFlujosDescontadosInversion(), other.getFlujosDescontadosInversion()),
                sum(base.getFlujosDescontadosEgresos(), other.getFlujosDescontadosEgresos()),
                sum(base.getFlujosDescontadosIngresos(), other.getFlujosDescontadosIngresos()),
                sum(base.getFlujosDescontadosCostos(), other.getFlujosDescontadosCostos()));
    }

    private BigDecimal add(BigDecimal a, BigDecimal b) {
        if (a == null)
            return b;
        if (b == null)
            return a;
        return a.add(b);
    }

    private static double sum(Double a, Double b) {
        double aValue = (a != null && !Double.isNaN(a)) ? a : 0.0;
        double bValue = (b != null && !Double.isNaN(b)) ? b : 0.0;
        return aValue + bValue;
    }
}