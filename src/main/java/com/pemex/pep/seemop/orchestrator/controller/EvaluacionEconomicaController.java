package com.pemex.pep.seemop.orchestrator.controller;

import jakarta.inject.Inject;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;
import com.pemex.pep.seemop.orchestrator.models.EvaluacionResponse;
import com.pemex.pep.seemop.orchestrator.services.EvaluacionEconomicaService;
import com.pemex.pep.seemop.orchestrator.services.ProductionProfileService;

@Path("/api/v1")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
public class EvaluacionEconomicaController {

	@Inject
	EvaluacionEconomicaService evaluacionEconomicaService;

	@Inject
	ProductionProfileService productionProfileService;

	@GET
	@Path("/getEvaluacionEconomica/{idOportunidad}/version/{version}/{cuota}/{declinada}/{pce}/{area}/{plataformadesarrollo}/{lineadedescarga}/{estacioncompresion}/{ducto}/{bateria}/{infra}/{perf}/{term}/{infraDES}/{perfDES}/{termDES}/{ArbolesSubmarinos}/{manifolds}/{risers}/{sistemaDeControl}/{cubiertaDeProceso}/{buquetaqueCompra}/{buquetaqueRenta}/{productionProfileFlag}")
	public EvaluacionResponse evaluacionEconomica(
			@PathParam("idOportunidad") Integer idOportunidad,
			@PathParam("version") Integer version,
			@PathParam("cuota") double cuota,
			@PathParam("declinada") double declinada,
			@PathParam("pce") double pce,
			@PathParam("area") double area,
			@PathParam("plataformadesarrollo") double plataformadesarrollo,
			@PathParam("lineadedescarga") double lineadedescarga,
			@PathParam("estacioncompresion") double estacioncompresion,
			@PathParam("ducto") double ducto,
			@PathParam("bateria") double bateria,
			@PathParam("infra") double infra,
			@PathParam("perf") double perf,
			@PathParam("term") double term,
			@PathParam("infraDES") double infraDES,
			@PathParam("perfDES") double perfDES,
			@PathParam("termDES") double termDES,
			@PathParam("ArbolesSubmarinos") double arbolesSubmarinos,
			@PathParam("manifolds") double manifolds,
			@PathParam("risers") double risers,
			@PathParam("sistemaDeControl") double sistemaDeControl,
			@PathParam("cubiertaDeProceso") double cubiertaDeProceso,
			@PathParam("buquetaqueCompra") double buquetaqueCompra,
			@PathParam("buquetaqueRenta") double buquetaqueRenta,
			@PathParam("productionProfileFlag") int productionProfileFlag
	) {
		if (productionProfileFlag != 1) {
			return evaluacionEconomicaService.getInfoPozosService(
				idOportunidad, version, cuota, declinada, pce, area,plataformadesarrollo, lineadedescarga, estacioncompresion,
				ducto,bateria,  infra, perf, term,
				infraDES, perfDES, termDES, arbolesSubmarinos, manifolds, risers,
				sistemaDeControl, cubiertaDeProceso, buquetaqueCompra, buquetaqueRenta
			); 
		}
		return productionProfileService.processSimpleEvaluaciones(
				idOportunidad, version, cuota, declinada, pce, area,plataformadesarrollo, lineadedescarga, estacioncompresion,
				ducto,bateria,  infra, perf, term,
				infraDES, perfDES, termDES, arbolesSubmarinos, manifolds, risers,
				sistemaDeControl, cubiertaDeProceso, buquetaqueCompra, buquetaqueRenta
		); 
	}

}
