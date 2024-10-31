package mx.com.innovating.cloud.orchestrator.clients;

import java.util.List;

import mx.com.innovating.cloud.data.entities.InformacionOportunidad;
import org.eclipse.microprofile.rest.client.inject.RegisterRestClient;

import io.quarkus.rest.client.reactive.ClientExceptionMapper;
import io.smallrye.mutiny.Uni;
import jakarta.ws.rs.GET;
import jakarta.ws.rs.Path;
import jakarta.ws.rs.PathParam;
import jakarta.ws.rs.Produces;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import mx.com.innovating.cloud.orchestrator.exeptions.RestClientExecutionException;
import mx.com.innovating.cloud.orchestrator.models.CostoOperacion;
import mx.com.innovating.cloud.orchestrator.models.EscaleraProduccion;
import mx.com.innovating.cloud.orchestrator.models.FactorInversion;
import mx.com.innovating.cloud.orchestrator.models.FactorInversionDesarrollo;
import mx.com.innovating.cloud.orchestrator.models.FactorInversionExploratorio;
import mx.com.innovating.cloud.orchestrator.models.InformacionInversion;
import mx.com.innovating.cloud.orchestrator.models.Paridad;
import mx.com.innovating.cloud.orchestrator.models.PozosActivos;
import mx.com.innovating.cloud.orchestrator.models.PrecioHidrocarburo;
import mx.com.innovating.cloud.orchestrator.models.ProduccionTotalMmbpce;
import mx.com.innovating.cloud.orchestrator.models.VectorProduccion;

@Path("/api/v1")
@RegisterRestClient(configKey = "oportunities-client")
public interface DatabaseConnectorClient {

	@GET
	@Produces(MediaType.APPLICATION_JSON)
	@Path("/getVectorProduccion/{idOportunidad}")
	Uni<List<VectorProduccion>> getVectorProduccion(@PathParam("idOportunidad") Integer idOportunidad);

	@GET
	@Produces(MediaType.APPLICATION_JSON)
	@Path("/getPozosActivos/{idOportunidad}")
	Uni<List<PozosActivos>> getPozosActivos(@PathParam("idOportunidad") Integer idOportunidad);

	@GET
	@Produces(MediaType.APPLICATION_JSON)
	@Path("/getPozosPerforados/{idOportunidad}")
	Uni<List<EscaleraProduccion>> getPozosTerminados(@PathParam("idOportunidad") Integer idOportunidad);

	@GET
	@Produces(MediaType.APPLICATION_JSON)
	@Path("/getFactorInversion/{idOportunidad}")
	Uni<FactorInversion> getFactorInversion(@PathParam("idOportunidad") Integer idOportunidad);

	@GET
	@Produces(MediaType.APPLICATION_JSON)
	@Path("/getInfoOportunidad/{idOportunidad}")
	Uni<InformacionOportunidad> getInfoOportunidad(@PathParam("idOportunidad") Integer idOportunidad);

	@GET
	@Produces(MediaType.APPLICATION_JSON)
	@Path("/getParidad/{anioInicio}")
	Uni<Paridad> getParidad(@PathParam("anioInicio") String anioInicio);

	@GET
	@Produces(MediaType.APPLICATION_JSON)
	@Path("/getPrecioHidrocarburo/{idOportunidad}/programa/{idPrograma}")
	Uni<List<PrecioHidrocarburo>> getPrecioHidrocarburo(@PathParam("idOportunidad") Integer idOportunidad,
			@PathParam("idPrograma") Integer idPrograma);

	@GET
	@Produces(MediaType.APPLICATION_JSON)
	@Path("/getFactorInversionExploratorio/{idOportunidad}")
	Uni<FactorInversionExploratorio> getFactorInversionExploratorio(@PathParam("idOportunidad") Integer idOportunidad);

	@GET
	@Produces(MediaType.APPLICATION_JSON)
	@Path("/getFactorInversionDesarrollo/{idOportunidad}")
	Uni<FactorInversionDesarrollo> getFactorInversionDesarrollo(@PathParam("idOportunidad") Integer idOportunidad);

	@GET
	@Produces(MediaType.APPLICATION_JSON)
	@Path("/getInformacionInversion/{idOportunidad}")
	Uni<InformacionInversion> getInformacionInversion(@PathParam("idOportunidad") Integer idOportunidad);

	@GET
	@Produces(MediaType.APPLICATION_JSON)
	@Path("/getCostoOperacion/{idProyecto}")
	Uni<List<CostoOperacion>> getCostoOperacion(@PathParam("idProyecto") Integer idProyecto);

	@GET
	@Produces(MediaType.APPLICATION_JSON)
	@Path("/getProduccionTotalMmbpce/{idOportunidad}")
	Uni<ProduccionTotalMmbpce> getProduccionTotalMmbpce(@PathParam("idOportunidad") Integer idOportunidad);

	@ClientExceptionMapper
	static RuntimeException toException(Response response) {

		if (response.getStatus() == 500) {
			return new RestClientExecutionException("The remote service responded with HTTP 500");
		}

		if (response.getStatus() == 404) {
			return new RestClientExecutionException(
					"No se encontraron el la BD los datos necesarios para hacer la Evaluacion Economica");
		}

		return null;
	}

}
