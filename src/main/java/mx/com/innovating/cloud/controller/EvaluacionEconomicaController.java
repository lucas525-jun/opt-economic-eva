package mx.com.innovating.cloud.controller;

import io.smallrye.mutiny.Uni;
import jakarta.inject.Inject;
import jakarta.ws.rs.Consumes;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;
import mx.com.innovating.cloud.models.EvaluacionResponse;
import mx.com.innovating.cloud.services.EvaluacionEconomicaService;

@Path("/api/v1")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
public class EvaluacionEconomicaController {

	@Inject
	EvaluacionEconomicaService evaluacionEconomicaService;

	@GET
	@Path("/getEvaluacionEconomica/{idOportunidad}")
	public Uni<EvaluacionResponse> evaluacionEconomica(@PathParam("idOportunidad") Integer idOportunidad) {
		return evaluacionEconomicaService.getInfoOportunidad(idOportunidad).onItem()
				.transformToUni(res -> evaluacionEconomicaService.getInfoPozosService(idOportunidad, res));
	}

}
