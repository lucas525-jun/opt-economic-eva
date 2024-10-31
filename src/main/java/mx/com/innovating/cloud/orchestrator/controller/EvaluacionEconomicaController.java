package mx.com.innovating.cloud.orchestrator.controller;

import jakarta.inject.Inject;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;
import mx.com.innovating.cloud.data.entities.InformacionOportunidad;
import mx.com.innovating.cloud.orchestrator.models.EvaluacionResponse;
import mx.com.innovating.cloud.orchestrator.services.EvaluacionEconomicaService;

@Path("/api/v1")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
public class EvaluacionEconomicaController {

	@Inject
	EvaluacionEconomicaService evaluacionEconomicaService;

	@GET
	@Path("/getEvaluacionEconomica/{idOportunidad}/version/{version}")
	public EvaluacionResponse evaluacionEconomica(@PathParam("idOportunidad") Integer idOportunidad, @PathParam("version") Integer version) {

		return evaluacionEconomicaService.getInfoPozosService(idOportunidad, version);
	}

}
