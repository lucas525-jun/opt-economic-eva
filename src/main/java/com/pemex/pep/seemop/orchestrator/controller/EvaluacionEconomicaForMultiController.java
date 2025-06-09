package com.pemex.pep.seemop.orchestrator.controller;

import jakarta.inject.Inject;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;
import com.pemex.pep.seemop.orchestrator.models.EvaluacionResponse;
import com.pemex.pep.seemop.orchestrator.services.EvaluacionEconomicaMultiService;
import com.pemex.pep.seemop.orchestrator.services.ProductionProfileService;

import java.util.List;

@Path("/api/v1")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
public class EvaluacionEconomicaForMultiController {

	@Inject
	EvaluacionEconomicaMultiService evaluacionEconomicaMultiService;

	@Inject
	ProductionProfileService productionProfileService;

	@POST
	@Path("/getEvaluacionEconomicaMulti")
	public EvaluacionResponse evaluacionEconomicaMulti(List<List<Object>> parameterList) {
		EvaluacionResponse result = evaluacionEconomicaMultiService.processMultipleEvaluaciones(parameterList);
		return result;
	}

	@POST
	@Path("/getEvaluacionPerfilDeProduccion")
	public EvaluacionResponse evaluacionPerfilDeProducción(List<List<Object>> parameterList) {
		EvaluacionResponse result = productionProfileService.processMultipleEvaluaciones(parameterList);
		return result;
	}

}