package mx.com.innovating.cloud.data.repository;

import io.quarkus.logging.Log;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.persistence.EntityManager;
import jakarta.ws.rs.PathParam;
import mx.com.innovating.cloud.data.entities.InformacionOportunidad;
import mx.com.innovating.cloud.data.exceptions.SqlExecutionErrorException;
import mx.com.innovating.cloud.data.models.*;

import java.util.List;
import java.util.Optional;

@ApplicationScoped
public class DataBaseConnectorRepository {

    @Inject
    EntityManager em;

    public List<VectorProduccion> getVectorProduccion(Integer idOportunidad) {
        // ToDo: pasar idVersion como parametro
        try {
            final var queryString = "SELECT * FROM calculo.spp_produccionanual(8, :idOportunidad, 2)";
            return em.createNativeQuery(queryString, VectorProduccion.class)
                    .setParameter("idOportunidad", idOportunidad).getResultStream().toList();
        } catch (Exception e) {
            Log.error("JDBC: getVectorProduccion exception executing SQL",e);
            throw new SqlExecutionErrorException("JDBC: getVectorProduccion exception executing SQL");
        }
    }

    public List<ProduccionPozos> getProduccionPozo(Integer idOportunidad) {
        // ToDo: pasar idVersion como parametro

        try {
            final var queryString = "SELECT * FROM calculo.spp_pozovolumen(8, :idOportunidad, 2)";
            return em.createNativeQuery(queryString, ProduccionPozos.class)
                    .setParameter("idOportunidad", idOportunidad).getResultStream().toList();
        } catch (Exception e) {
            Log.error("JDBC: getProduccionPozo exception executing SQL",e);
            throw new SqlExecutionErrorException("JDBC: getProduccionPozo exception executing SQL");
        }

    }

    public List<EscaleraProduccion> getEscaleraProduccion(Integer idOportunidad) {
        // ToDo: pasar idVersion como parametro

        try {
            final var queryString = "SELECT * from calculo.spp_escaleraproduccion(8, :idOportunidad, 2)";
            return em.createNativeQuery(queryString, EscaleraProduccion.class)
                    .setParameter("idOportunidad", idOportunidad).getResultStream().toList();
        } catch (Exception e) {
            Log.error("JDBC: getEscaleraProduccion exception executing SQL",e);
            throw new SqlExecutionErrorException("JDBC getEscaleraProduccion exception executing SQL");
        }

    }

    public List<EscaleraProduccion> getPozosPerforados(Integer idOportunidad) {
        // ToDo: pasar idVersion como parametro
        try {
            final var queryString = """
                        SELECT *
                        FROM calculo.spp_escaleraproduccion(8, :idOportunidad, 2) t
                        WHERE vidconsecutivo = (
                            SELECT MIN(vidconsecutivo)
                            FROM calculo.spp_escaleraproduccion(8, :idOportunidad, 2) t2
                            WHERE t2.vidpozo = t.vidpozo
                        )
                        ORDER BY vidpozo
                    """;
            return em.createNativeQuery(queryString, EscaleraProduccion.class)
                    .setParameter("idOportunidad", idOportunidad).getResultStream().toList();
        } catch (Exception e) {
            Log.error("JDBC: getPozosPerforados exception executing SQL",e);
            throw new SqlExecutionErrorException("JDBC getPozosPerforados exception executing SQL");
        }

    }

    public List<PozosActivos> getPozosActivos(Integer idOportunidad) {
        // ToDo: pasar idVersion como parametro
        try {
            final var queryString = """
                    SELECT vanio, ROUND(COUNT(*) /12.0, 3) AS promedio_anual
                                  FROM (
                                           select * from calculo.spp_escaleraproduccion(8, :idOportunidad, 2) ) t
                                  GROUP BY vanio
                                  ORDER BY vanio;
                    """;
            return em.createNativeQuery(queryString, PozosActivos.class).setParameter("idOportunidad", idOportunidad)
                    .getResultStream().toList();
        } catch (Exception e) {
            Log.error("JDBC: getPozosActivos exception executing SQL",e);
            throw new SqlExecutionErrorException("JDBC getPozosActivos exception executing SQL");
        }

    }

    public List<PrecioHidrocarburo> getPrecioHidrocarburo(Integer idOportunidad, Integer idPrograma) {

        try {
            final var queryString = """
                    SELECT
                        idhidrocarburo,
                        idoportunidadobjetivo,
                        hidrocarburo,
                        anioprecio,
                        precio
                    FROM
                        catalogo.volumetriaoportunidadfactoresper50vw
                    WHERE
                        idoportunidadobjetivo = :idOportunidad
                    AND
                        idtipovalor = 2
                    AND
                        idprograma = :idPrograma
                    ORDER BY anioprecio ASC
                    """;
            return em.createNativeQuery(queryString, PrecioHidrocarburo.class)
                    .setParameter("idOportunidad", idOportunidad).setParameter("idPrograma", idPrograma)
                    .getResultStream().toList();
        } catch (Exception e) {
            Log.error("JDBC: getPrecioHidrocarburo exception executing SQL",e);
            throw new SqlExecutionErrorException("JDBC getPrecioHidrocarburo exception executing SQL");
        }

    }

    public Paridad getParidad(Integer anio) {

        try {
            final var queryString = "SELECT paridad FROM catalogo.premisastbl WHERE año = :anio";
            Optional<Paridad> result = em.createNativeQuery(queryString, Paridad.class).setParameter("anio", anio).getResultStream()
                    .findFirst();
            if(result.isEmpty()){
                Log.error("Paridad: Value no present with anio = " + anio);
                throw new SqlExecutionErrorException("Paridad: Value no present with anio = " + anio);
            }
            return result.get();
        } catch (Exception e) {
            Log.error("JDBC: getParidad exception executing SQL",e);
            throw new SqlExecutionErrorException("JDBC getParidadexception executing SQL");
        }

    }

    public FactorInversionExploratorio getFactorInversionExploratorio(Integer idOportunidad) {

        try {
            final var queryString = """
                    select *
                    from inversion.exploratoriooportunidadvw
                    where idoportunidadobjetivo = :idOportunidad
                    and idtipovalor=2
                    """;
            Optional<FactorInversionExploratorio> result =em.createNativeQuery(queryString, FactorInversionExploratorio.class)
                    .setParameter("idOportunidad", idOportunidad).getResultStream().findFirst();
            if( result.isEmpty()){
                Log.error("FactorInversionExploratorio: Value no present with idOportunidad = " + idOportunidad);
                throw new SqlExecutionErrorException("FactorInversionExploratorio: Value no present with idOportunidad = " + idOportunidad);
            }
            return result.get();
        } catch (Exception e) {
            Log.error("JDBC: getFactorInversionExploratorio exception executing SQL",e);
            throw new SqlExecutionErrorException("JDBC getFactorInversionExploratorio exception executing SQL");
        }

    }

    public FactorInversionDesarrollo getFactorInversionDesarrollo(Integer idOportunidad) {

        try {
            final var queryString = """
                    select *
                    from inversion.desarrollooportunidadvw
                    where idoportunidadobjetivo = :idOportunidad
                    and idtipovalor=2
                    """;
            Optional<FactorInversionDesarrollo> result =  em.createNativeQuery(queryString, FactorInversionDesarrollo.class)
                    .setParameter("idOportunidad", idOportunidad).getResultStream().findFirst();

            if(result.isEmpty()){
                Log.error("FactorInversionDesarrollo: Value no present with idOportunidad = " + idOportunidad);
                throw new SqlExecutionErrorException("FactorInversionDesarrollo: Value no present with idOportunidad = " + idOportunidad);
            }

            return result.get();

        } catch (Exception e) {
            Log.error("JDBC: getFactorInversionDesarrollo exception executing SQL",e);
            throw new SqlExecutionErrorException("JDBC getFactorInversionDesarrollo exception executing SQL");
        }

    }

    public ProduccionTotalMmbpce getProduccionTotalMmbpce(Integer idOportunidad) {
        // ToDo: pasar idVersion como parametro

        try {
            final var queryString = """
                    SELECT SUM(vproduccion) AS total 
                    FROM calculo.spp_escaleraproduccion(8, :idOportunidad, 2);
                    """;
            Optional<ProduccionTotalMmbpce> result = em.createNativeQuery(queryString, ProduccionTotalMmbpce.class)
                    .setParameter("idOportunidad", idOportunidad).getResultStream().findFirst();

            if(result.isEmpty()){
                Log.error("JDBC exception: getProduccionTotalMmbpce Value no present with idOportunidad = " + idOportunidad);
                throw new SqlExecutionErrorException("JDBC exception: VgetProduccionTotalMmbpce alue no present with idOportunidad = " + idOportunidad);
            }

            return result.get();

        } catch (Exception e) {
            Log.error("JDBC: getProduccionTotalMmbpce exception executing SQL",e);
            throw new SqlExecutionErrorException("JDBC getProduccionTotalMmbpce exception executing SQL");
        }

    }

    public InformacionInversion getInformacionInversion(Integer idOportunidad) {

        try {
            final var queryString = """
                    select *
                    from inversion.otrosdatosvw
                    where idoportunidadobjetivo = :idOportunidad
                    and idtipovalor=2
                    """;
            Optional<InformacionInversion> result = em.createNativeQuery(queryString, InformacionInversion.class)
                    .setParameter("idOportunidad", idOportunidad).getResultStream().findFirst();

            if(result.isEmpty()){
                Log.error("JDBC exception: getInformacionInversion Value no present with idOportunidad = " + idOportunidad);
                throw new SqlExecutionErrorException("JDBC exception: getInformacionInversion Value no present with idOportunidad = " + idOportunidad);
            }
            return result.get();
        } catch (Exception e) {
            Log.error("JDBC getInformacionInversion exception executing SQL",e);
            throw new SqlExecutionErrorException("JDBC getInformacionInversion exception executing SQL");
        }

    }

    public FactorInversion getFactorInversion(Integer idOportunidad) {

        try {
            final var queryString = """
                    SELECT DISTINCT 
                        idhidrocarburo,
                        idoportunidadobjetivo,
                        hidrocarburo,
                        COALESCE(p_pce,0),
                        COALESCE(factor_aceite,0),
                        COALESCE(factor_gas,0),
                        COALESCE(factor_condensado,0),
                        anioprecio
                    FROM
                        catalogo.volumetriaoportunidadfactoresper50vw
                    WHERE
                        idoportunidadobjetivo = :idOportunidad
                     AND
                        idtipovalor = 2      
                    """;
            Optional<FactorInversion> result = em.createNativeQuery(queryString, FactorInversion.class).setParameter("idOportunidad", idOportunidad)
                    .getResultStream().findFirst();
            if(result.isEmpty()){
                Log.error("FactorInversion: Value no present with idOportunidad = " + idOportunidad);
                throw new SqlExecutionErrorException("FactorInversion: Value no present with idOportunidad = " + idOportunidad);
            }
            return result.get();
        } catch (Exception e) {
            Log.error("JDBC: getFactorInversion exception executing SQL",e);
            throw new SqlExecutionErrorException("JDBC getInformacionInversion exception executing SQL");
        }

    }

    public List<CostoOperacion> getCostoOperacion(Integer idProyecto) {

        try {
            final var queryString = """
                    SELECT idcostooperacion, costooperacion, fecha, gasto
                    FROM costo.operacionproyectovw
                    WHERE idproyecto = :idProyecto
                    AND idtipovalor=2
                    """;
            List<CostoOperacion> result =  em.createNativeQuery(queryString, CostoOperacion.class)
                    .setParameter("idProyecto", idProyecto).getResultStream().toList();
            return result;
        } catch (Exception e) {
            Log.error("JDBC: getCostoOperacion exception executing SQL",e);
            throw new SqlExecutionErrorException("JDBC getCostoOperacion exception executing SQL");
        }

    }

    public List<Oportunidades> getOportunidadesByNombreVersion(String nombreVersion) {

        try {
            final var queryString = """
                    SELECT c.idoportunidadobjetivo, a.idoportunidad, a.oportunidad, b.nombreversion
                    FROM catalogo.oportunidadvw a
                    join catalogo.versiontbl b on a.idversion = b.idversion
                    join catalogo.claveobjetivovw c on a.idoportunidad = c.idoportunidad
                    where nombreversion = :nombreVersion
                    """;
            return em.createNativeQuery(queryString, Oportunidades.class)
                    .setParameter("nombreVersion", nombreVersion)
                    .getResultStream().toList();
        } catch (Exception e) {
            Log.error("JDBC: getOportunidadesByNombreVersion exception executing SQL", e);
            throw new SqlExecutionErrorException("JDBC getOportunidadesByNombreVersion exception executing SQL");
        }

    }

    public InformacionOportunidad getInfoOportunidad(Integer idOportunidad) {
        return InformacionOportunidad.findByIdoportunidadobjetivo(idOportunidad);
    }

}
