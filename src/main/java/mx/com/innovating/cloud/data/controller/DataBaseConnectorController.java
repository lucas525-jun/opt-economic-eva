package mx.com.innovating.cloud.data.controller;

import java.io.IOException;

import jakarta.inject.Inject;
import jakarta.validation.constraints.NotNull;
import jakarta.ws.rs.*;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import mx.com.innovating.cloud.data.entities.InformacionOportunidad;
import mx.com.innovating.cloud.data.exceptions.SqlNotFoundException;
import mx.com.innovating.cloud.data.repository.DataBaseConnectorRepository;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.logging.Logger;

@Path("/api/v1/connector")
@Produces(MediaType.APPLICATION_JSON)
@Consumes(MediaType.APPLICATION_JSON)
public class DataBaseConnectorController {

	@Inject
	DataBaseConnectorRepository dbRepository;




	@GET
	@Path("/getPlanDesarrollo")
	public Response listPlanDesarrolloByOportunidad(@NotNull @QueryParam("idOportunidad") Integer idOportunidad, @NotNull @QueryParam("idVersion") Integer idVersion) {
		var result = dbRepository.getPlanDesarrolloByOportunidad(idOportunidad, idVersion);
		if(result.isEmpty()){
			throw new SqlNotFoundException("PlanDesarrollo: Value no present with idOportunidad = " + idOportunidad);
		}
		return Response.ok(result).build();
	}

	@GET
	@Path("/getVectorProduccion/{idOportunidad}/version/{version}/{cuota}/{declinada}/{pce}/{area}")
	public Response listVectorProduccion(@PathParam("idOportunidad") Integer idOportunidad, @PathParam("version") Integer version, @PathParam("cuota") double cuota, @PathParam("declinada") double declinada, @PathParam("pce") double pce, @PathParam("area") double area) {
		var result = dbRepository.getVectorProduccion(idOportunidad, version,cuota,declinada,pce,area);
		if(result.isEmpty()){
			throw new SqlNotFoundException("VectorProduccion: Value no present with idOportunidad = " + idOportunidad);
		}
		return Response.ok(result).build();
	}

	@GET
	@Path("/getOportunidades")
	public Response listOportunidadesByNombreVersion(@NotNull @QueryParam("nombreVersion") String nombreVersion) {
		var result = dbRepository.getOportunidadesByNombreVersion(nombreVersion);
		if(result.isEmpty()){
			throw new SqlNotFoundException("OportunidadesByNombreVersion: Value no present with nombreVersion = " + nombreVersion);
		}
		return Response.ok(result).build();
	}

	//@GET
	//@Path("/getProduccionPozo/{idOportunidad}/version/{version}")
	//public Response listProduccionPozo(@PathParam("idOportunidad") Integer idOportunidad, @PathParam("version") Integer version){
	//	var result = dbRepository.getProduccionPozo(idOportunidad, version);
	//	if(result.isEmpty()){
	//		throw new SqlNotFoundException("ProduccionPozo: Value no present");
	//	}
	//	return Response.ok(result).build();
	//}

	//@GET
	//@Path("/getEscaleraProduccion/{idOportunidad}/version/{version}")
	//public Response listEscaleraProduccion(@PathParam("idOportunidad") Integer idOportunidad, @PathParam("version") Integer version) {
	//	var result = dbRepository.getEscaleraProduccion(idOportunidad, version);
	//	if(result.isEmpty()){
	//		throw new SqlNotFoundException("EscaleraProduccion: Value no present with idOportunidad = " + idOportunidad);
	//}
	//return Response.ok(result).build();
	//}

	@GET
	@Path("/getPozosPerforados/{idOportunidad}/version/{version}/{cuota}/{declinada}/{pce}/{area}")
	public Response listPozosPerforados(@PathParam("idOportunidad") Integer idOportunidad, @PathParam("version") Integer version, @PathParam("cuota") double cuota, @PathParam("declinada") double declinada, @PathParam("pce") double pce, @PathParam("area") double area) {
		var result = dbRepository.getPozosPerforados(idOportunidad, version, cuota, declinada, pce, area);
		if(result.isEmpty()){
			throw new SqlNotFoundException("PozosPerforados: Value no present with idOportunidad = " + idOportunidad);
		}
		return Response.ok(result).build();
	}

	//@GET
	//@Path("/getPozosActivos/{idOportunidad}/version/{version}")
	//public Response listPozosActivos(@PathParam("idOportunidad") Integer idOportunidad, @PathParam("version") Integer version) {
	//	var result = dbRepository.getPozosActivos(idOportunidad, version);
	//	if(result.isEmpty()){
	//		throw new SqlNotFoundException("PozosActivos: Value no present with idOportunidad = " + idOportunidad);
	//	}
	//	return Response.ok(result).build();
	//}

	@GET
	@Path("/getCostoOperacion/{idProyecto}")
	public Response listCostoOperacion(@PathParam("idProyecto") Integer idProyecto) {
		var result = dbRepository.getCostoOperacion(idProyecto);
		if(result.isEmpty()){
			throw new SqlNotFoundException("CostoOperacion: Value no present with idProyecto = " + idProyecto);
		}
		return Response.ok(result).build();
	}

	@GET
	@Path("/getInfoOportunidad/{idOportunidad}")
	public Response informacioOportunidad(@PathParam("idOportunidad") Integer idOportunidad) {
		var result = InformacionOportunidad.findByIdoportunidadobjetivo(idOportunidad);
		if(result == null){
			throw new SqlNotFoundException("InformacioOportunidad: Value no present with idOportunidad = " + idOportunidad);
		}
		return Response.ok(result).build();
	}

	@GET
	@Path("/getFactorInversion/{idOportunidad}")
	public Response factorInversion(@PathParam("idOportunidad") Integer idOportunidad) {
		var result = dbRepository.getFactorInversion(idOportunidad);
		if(result != null){
			return Response.ok(result).build();
		} else {
			throw new SqlNotFoundException("FactorInversion: Value no present with idOportunidad = " + idOportunidad);
		}
	}

	@GET
	@Path("/getPrecioHidrocarburo/{idOportunidad}/programa/{idPrograma}")
	public Response listPrecioHidrocarburo(@PathParam("idOportunidad") Integer idOportunidad,
										   @PathParam("idPrograma") Integer idPrograma) {
		var result = dbRepository.getPrecioHidrocarburo(idOportunidad, idPrograma);
		if(result.isEmpty()){
			throw new SqlNotFoundException("PrecioHidrocarburo: Value no present with idOportunidad = " + idOportunidad);
		}
		return Response.ok(result).build();
	}

	@GET
	@Path("/getParidad/{anioInicio}")
	public Response paridad(@PathParam("anioInicio") Integer anioInicio) {
		var result = dbRepository.getParidad(anioInicio);
		if(result != null){
			return Response.ok(result).build();
		} else {
			throw new SqlNotFoundException("Paridad: Value no present with anioInicio = " + anioInicio);
		}
	}

	@GET
	@Path("/getFactorInversionExploratorio/{idOportunidad}")
	public Response factorInversionExploratorio(@PathParam("idOportunidad") Integer idOportunidad) {
		var result = dbRepository.getFactorInversionExploratorio(idOportunidad);
		if(result != null){
			return Response.ok(result).build();
		} else {
			throw new SqlNotFoundException("FactorInversionExploratorio: Value no present with idOportunidad = " + idOportunidad);
		}
	}


	@GET
	@Path("/getFactorInversionDesarrollo/{idOportunidad}")
	public Response factorInversionDesarrollo(@PathParam("idOportunidad") Integer idOportunidad) {
		var result = dbRepository.getFactorInversionDesarrollo(idOportunidad);
		if(result != null){
			return Response.ok(result).build();
		} else {
			throw new SqlNotFoundException("FactorInversionDesarrollo: Value no present with idOportunidad = " + idOportunidad);
		}
	}


	//@GET
	//@Path("/getProduccionTotalMmbpce/{idOportunidad}/version/{version}")
	//public Response produccionTotalMmbpce(@PathParam("idOportunidad") Integer idOportunidad, @PathParam("version") Integer version) {
	//	var result = dbRepository.getProduccionTotalMmbpce(idOportunidad, version);
	//	if(result != null){
	//		return Response.ok(result).build();
	//	} else {
	//throw new SqlNotFoundException("JDBC exception: Value no present with idOportunidad = " + idOportunidad);
	//	}
	//}

	@GET
	@Path("/getInformacionInversion/{idOportunidad}")
	public Response informacionInversion(@PathParam("idOportunidad") Integer idOportunidad) {
		var result = dbRepository.getInformacionInversion(idOportunidad);
		if(result != null){
			return Response.ok(result).build();
		} else {
			throw new SqlNotFoundException("JDBC exception: Value no present with idOportunidad = " + idOportunidad);
		}
	}


}
