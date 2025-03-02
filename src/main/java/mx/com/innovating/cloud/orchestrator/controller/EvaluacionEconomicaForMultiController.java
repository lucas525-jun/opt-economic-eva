package mx.com.innovating.cloud.orchestrator.controller;

import jakarta.inject.Inject;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.POST;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;
import mx.com.innovating.cloud.orchestrator.models.EvaluacionResponse;
import mx.com.innovating.cloud.orchestrator.services.EvaluacionEconomicaMultiService;

import java.util.List;

@Path("/api/v1")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
public class EvaluacionEconomicaForMultiController {

	@Inject
	EvaluacionEconomicaMultiService evaluacionEconomicaMultiService;

	@POST
	@Path("/getEvaluacionEconomicaMulti")
	public EvaluacionResponse evaluacionEconomicaMulti(List<List<Object>> parameterList) {
		EvaluacionResponse result = evaluacionEconomicaMultiService.processMultipleEvaluaciones(parameterList);

		// if (result.getEvaluacionEconomica() != null &&
		// result.getEvaluacionEconomica().size() > 1) {
		// double ducto =
		// result.getEvaluacionEconomica().get(0).getInversiones().getDuctos();

		// double ducto2 =
		// result.getEvaluacionEconomica().get(1).getInversiones().getDuctos();
		// System.err.println("ducto: " + ducto);
		// System.err.println("ducto2: " + ducto2);
		// } else {
		// }

		return result;
	}

}