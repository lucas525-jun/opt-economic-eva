package com.pemex.pep.seemop.orchestrator.controller;

import jakarta.inject.Inject;
import jakarta.ws.rs.*;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import com.pemex.pep.seemop.orchestrator.services.ImpuestosService;

@Path("/api/tax")
public class ImpuestosController {

    @Inject
    ImpuestosService impuestosServices;

    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces("application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
    @Path("/createExcelImpuestos/{idOportunidad}/version/{version}")
    public Response createExcelImpuestos(@PathParam("idOportunidad") Integer idOportunidad, @PathParam("version") Integer version) {
        try {

            byte[] excel = impuestosServices.generateExcelImpuestos(idOportunidad, version);
            return Response.ok(excel)
                    .header("Content-Disposition", "attachment; filename=impuestos_"+idOportunidad+".xlsx")
                    .build();

        }catch (Exception e){
            e.printStackTrace();
            return Response.serverError().entity("Error al generar el archivo Excel").build();
        }

    }

}
