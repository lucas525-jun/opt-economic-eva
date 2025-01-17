package mx.com.innovating.cloud.data.repository;

import io.quarkus.logging.Log;

import io.quarkus.cache.CacheResult;
import io.quarkus.cache.CacheKey;
import jakarta.transaction.Transactional;

import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import jakarta.persistence.EntityManager;
import jakarta.persistence.Query;
import jakarta.ws.rs.PathParam;
import mx.com.innovating.cloud.data.entities.InformacionOportunidad;
import mx.com.innovating.cloud.data.exceptions.SqlExecutionErrorException;

// Core domain models from mx.com.innovating.cloud.data.models
import mx.com.innovating.cloud.data.models.OportunidadPlanDesarrollo;
import mx.com.innovating.cloud.data.models.PozosActivos;
import mx.com.innovating.cloud.data.models.VectorProduccion;
import mx.com.innovating.cloud.data.models.ProduccionPozos;
import mx.com.innovating.cloud.data.models.PrecioHidrocarburo;
import mx.com.innovating.cloud.data.models.Paridad;
import mx.com.innovating.cloud.data.models.FactorInversionExploratorio;
import mx.com.innovating.cloud.data.models.FactorInversionDesarrollo;
import mx.com.innovating.cloud.data.models.ProduccionTotalMmbpce;
import mx.com.innovating.cloud.data.models.InformacionInversion;
import mx.com.innovating.cloud.data.models.FactorInversion;
import mx.com.innovating.cloud.data.models.CostoOperacion;
import mx.com.innovating.cloud.data.models.Oportunidades;
import mx.com.innovating.cloud.data.models.EscaleraProduccion;

// Orchestrator models
import mx.com.innovating.cloud.orchestrator.models.Areakmasignacion;
import mx.com.innovating.cloud.orchestrator.models.FactorCalculo;

// Entity model
import mx.com.innovating.cloud.data.entities.InformacionOportunidad;

// Service classes for calculations
import mx.com.innovating.cloud.data.calculator.ProduccionAnualService;
import mx.com.innovating.cloud.data.calculator.EscaleraProduccionService;
import mx.com.innovating.cloud.data.calculator.PozoVolumenService;

// Exception handling
import mx.com.innovating.cloud.data.exceptions.SqlExecutionErrorException;

import java.sql.ResultSetMetaData;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.math.BigDecimal;
import java.math.RoundingMode;

@ApplicationScoped
public class DataBaseConnectorRepository {

    @Inject
    EntityManager em;

    @Inject
    ProduccionAnualService produccionAnualService;

    @Inject
    EscaleraProduccionService escaleraProduccionService;

    @Inject
    PozoVolumenService pozoVolumenService;

    @Transactional
    @CacheResult(cacheName = "plan-desarrollo-cache")

    public List<OportunidadPlanDesarrollo> getPlanDesarrolloByOportunidad(@CacheKey Integer idOportunidad,
            @CacheKey Integer idVersion) {
        try {
            final var queryString = """
                    SELECT distinct o.idoportunidad,
                    o.oportunidad,
                    r.idoportunidadobjetivo,
                    pd.idplandesarrollo,
                    pp.plandesarrollo,
                    pd.idinversion,
                    i.inversion,
                    pd.duracion ,
                    o.idversion
                    FROM catalogo.relinversionplantbl pd
                    JOIN catalogo.oportunidadtbl o ON pd.idplandesarrollo = o.idplandesarrollo
                    JOIN catalogo.plandesarrollotbl pp ON pd.idplandesarrollo = pp.idplandesarrollo
                    JOIN catalogo.inversiontbl i ON pd.idinversion = i.idinversion
                    JOIN catalogo.reloportunidadobjetivotbl r ON  r.idoportunidad = o.idoportunidad AND r.idversion = o.idversion
                    JOIN catalogo.versiontbl v ON v.idversion = :idVersion where r.idoportunidadobjetivo = :idOportunidad
                    """;
            List<OportunidadPlanDesarrollo> result = em.createNativeQuery(queryString, OportunidadPlanDesarrollo.class)
                    .setParameter("idOportunidad", idOportunidad)
                    .setParameter("idVersion", idVersion)
                    .getResultStream().toList();
            return result;
        } catch (Exception e) {
            Log.error("JDBC: getPlanDesarrolloByOportunidad exception executing SQL", e);
            throw new SqlExecutionErrorException("JDBC getCatalogo exception executing SQL");
        }
    }

    @Transactional
    @CacheResult(cacheName = "areakm-asignacion-cache")
    public Areakmasignacion getAreakmasignacion(@CacheKey Integer pnoportunidadobjetivo,
            @CacheKey Integer pnidversion) {
        try {
            final var queryString = """
                    SELECT
                        idoportunidadobjetivo,
                        idoportunidad,
                        idversion,
                        areakmasignacion,
                        pg
                    FROM
                        catalogo.reloportunidadobjetivotbl
                    WHERE
                        idversion = :pnidversion
                        AND idoportunidadobjetivo = :pnoportunidadobjetivo
                    """;

            Optional<Areakmasignacion> result = em.createNativeQuery(queryString, Areakmasignacion.class)
                    .setParameter("pnoportunidadobjetivo", pnoportunidadobjetivo)
                    .setParameter("pnidversion", pnidversion)
                    .getResultStream()
                    .findFirst();

            if (result.isEmpty()) {
                Log.error(
                        "ReloportunidadObjetivo: No data found with idoportunidadobjetivo = " + pnoportunidadobjetivo +
                                " and idversion = " + pnidversion);
                throw new SqlExecutionErrorException("ReloportunidadObjetivo: No data found with provided parameters.");
            }

            return result.get();
        } catch (Exception e) {
            Log.error("JDBC: getReloportunidadObjetivo exception executing SQL", e);
            throw new SqlExecutionErrorException("JDBC: getReloportunidadObjetivo exception executing SQL");
        }
    }

    @Transactional
    @CacheResult(cacheName = "factor-calculo-cache")
    public FactorCalculo getFactorCalculo(@CacheKey Integer pnoportunidadobjetivo, @CacheKey Integer pnidversion) {
        try {
            final var queryString = """
                    SELECT
                        idoportunidadobjetivo,
                        idversion,
                        COALESCE(mediaaceite / mediapce, 0) AS fc_aceite,
                        COALESCE(mediagas / mediapce, 0) AS fc_gas,
                        COALESCE(mediacondensado / mediapce, 0) AS fc_condensado
                    FROM
                        catalogo.mediavolumetriaoportunidadtbl
                    WHERE
                        idoportunidadobjetivo = :pnoportunidadobjetivo
                        AND idversion = :pnidversion
                    """;

            Optional<FactorCalculo> result = em.createNativeQuery(queryString, FactorCalculo.class)
                    .setParameter("pnoportunidadobjetivo", pnoportunidadobjetivo)
                    .setParameter("pnidversion", pnidversion)
                    .getResultStream()
                    .findFirst();

            if (result.isEmpty()) {
                Log.error("FactorCalculo: No data found with idoportunidadobjetivo = " + pnoportunidadobjetivo +
                        " and idversion = " + pnidversion);
                throw new SqlExecutionErrorException("FactorCalculo: No data found with provided parameters.");
            }

            return result.get();
        } catch (Exception e) {
            Log.error("JDBC: getFactorCalculo exception executing SQL", e);
            throw new SqlExecutionErrorException("JDBC: getFactorCalculo exception executing SQL");
        }
    }

    public List<VectorProduccion> getVectorProduccion(Integer pnoportunidadobjetivo, Integer pnidversion,
            double pncuota, double pndeclinada, double pnpce, double pnarea) {
        List<VectorProduccion> results = new ArrayList<>();

        results = produccionAnualService.calculateProduccionAnual(
                pnidversion, pnoportunidadobjetivo, pncuota,
                pndeclinada, pnpce, pnarea);

        return results;
    }

    public List<ProduccionPozos> getProduccionPozo(Integer idOportunidad, Integer version, Double pncuota,
            Double pndeclinada, Double pnpce) {
        List<ProduccionPozos> results = new ArrayList<>();

        results = pozoVolumenService.calcularPozoVolumen(
                version,
                idOportunidad,
                pncuota,
                pndeclinada,
                pnpce);

        return results;

    }

    public List<EscaleraProduccion> getEscaleraProduccion(Integer pnoportunidadobjetivo, Integer pnidversion,
            double pncuota, double pndeclinada, double pnpce, double pnarea) {
        List<EscaleraProduccion> results = new ArrayList<>();

        results = escaleraProduccionService.calculateEscaleraProduccion(
                pnidversion,
                pnoportunidadobjetivo,
                pncuota,
                pndeclinada,
                pnpce,
                pnarea);
        return results;

    }

    public List<EscaleraProduccion> getPozosPerforados(
            Integer pnoportunidadobjetivo,
            Integer pnidversion,
            double pncuota,
            double pndeclinada,
            double pnpce,
            double pnarea) {
        try {
            // Get all escalera produccion results
            List<EscaleraProduccion> allResults = escaleraProduccionService.calculateEscaleraProduccion(
                    pnidversion,
                    pnoportunidadobjetivo,
                    pncuota,
                    pndeclinada,
                    pnpce,
                    pnarea);

            // First, find minimum vidconsecutivo for each pozo
            Map<Integer, Integer> minConsecutivosPerPozo = allResults.stream()
                    .collect(Collectors.groupingBy(
                            EscaleraProduccion::getIdpozo,
                            Collectors.collectingAndThen(
                                    Collectors.minBy(Comparator.comparing(EscaleraProduccion::getIdconsecutivo)),
                                    opt -> opt.map(EscaleraProduccion::getIdconsecutivo).orElse(null))));

            // Then filter the original results to match those minimums
            return allResults.stream()
                    .filter(result -> result.getIdconsecutivo().equals(
                            minConsecutivosPerPozo.get(result.getIdpozo())))
                    .sorted(Comparator.comparing(EscaleraProduccion::getIdpozo))
                    .collect(Collectors.toList());
        } catch (Exception e) {
            Log.error("Error in getPozosPerforados", e);
            throw new SqlExecutionErrorException("Error in getPozosPerforados");
        }
    }

    public List<PozosActivos> getPozosActivos(
            Integer pnoportunidadobjetivo,
            Integer pnidversion,
            double pncuota,
            double pndeclinada,
            double pnpce,
            double pnarea) {
        try {
            // Get all escalera produccion results
            List<EscaleraProduccion> allResults = escaleraProduccionService.calculateEscaleraProduccion(
                    pnidversion,
                    pnoportunidadobjetivo,
                    pncuota,
                    pndeclinada,
                    pnpce,
                    pnarea);

            // Group by year and count
            Map<String, Long> countsByYear = allResults.stream()
                    .collect(Collectors.groupingBy(
                            EscaleraProduccion::getAnio,
                            Collectors.counting()));

            // Convert to final format
            List<PozosActivos> results = countsByYear.entrySet().stream()
                    .map(entry -> new PozosActivos(
                            entry.getKey(),
                            // Convert to BigDecimal and round to 3 decimal places
                            BigDecimal.valueOf(entry.getValue())
                                    .divide(BigDecimal.valueOf(12.0), 3, RoundingMode.HALF_UP)))
                    .sorted(Comparator.comparing(PozosActivos::getAnio))
                    .collect(Collectors.toList());

            return results;
        } catch (Exception e) {
            Log.error("Error in getPozosActivos", e);
            throw new SqlExecutionErrorException("Error in getPozosActivos");
        }
    }

    @Transactional
    @CacheResult(cacheName = "precio-hidrocarburo-cache")

    public List<PrecioHidrocarburo> getPrecioHidrocarburo(@CacheKey Integer idOportunidad,
            @CacheKey Integer idPrograma) {

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
            Log.error("JDBC: getPrecioHidrocarburo exception executing SQL", e);
            throw new SqlExecutionErrorException("JDBC getPrecioHidrocarburo exception executing SQL");
        }

    }

    @Transactional
    @CacheResult(cacheName = "paridad-cache")
    public Paridad getParidad(@CacheKey Integer anio) {

        try {
            final var queryString = "SELECT paridad FROM catalogo.premisastbl WHERE year = :anio";
            Optional<Paridad> result = em.createNativeQuery(queryString, Paridad.class).setParameter("anio", anio)
                    .getResultStream()
                    .findFirst();
            if (result.isEmpty()) {
                Log.error("Paridad: Value no present with anio = " + anio);
                throw new SqlExecutionErrorException("Paridad: Value no present with anio = " + anio);
            }
            return result.get();
        } catch (Exception e) {
            Log.error("JDBC: getParidad exception executing SQL", e);
            throw new SqlExecutionErrorException("JDBC getParidadexception executing SQL");
        }

    }

    @Transactional
    @CacheResult(cacheName = "factor-inversion-exploratorio-cache")
    public FactorInversionExploratorio getFactorInversionExploratorio(@CacheKey Integer idOportunidad) {
        try {
            final var queryString = """
                    SELECT *
                    FROM inversion.exploratoriooportunidadvw
                    WHERE idoportunidadobjetivo = :idOportunidad
                    AND idtipovalor = 2
                    """; // no se usa mas

            // Ejecutar la consulta nativa
            Query query = em.createNativeQuery(queryString);
            query.setParameter("idOportunidad", idOportunidad);

            // Obtener los resultados
            List<Object[]> resultList = query.getResultList();

            // Si la consulta devuelve resultados, obtener los metadatos
            if (!resultList.isEmpty()) {
                // Acceder a la primera fila (firstRow) para obtener el número de columnas
                Object[] firstRow = resultList.get(0);
                if (firstRow != null) {
                    // Obtener el número de columnas
                    int columnCount = firstRow.length;
                    System.out.println("Column count: " + columnCount);

                    // Imprimir los nombres de las columnas y su tipo
                    for (int i = 0; i < columnCount; i++) {
                        // Verificar si el valor es null
                        if (firstRow[i] != null) {
                            System.out.println("Column " + (i + 1) + ": " + firstRow[i].getClass().getName());
                        } else {
                            System.out.println("Column " + (i + 1) + ": null value");
                        }
                    }
                }
            } else {
                System.out.println("No results found.");
            }

            // Continuar con la consulta para mapear a la entidad si es necesario
            Optional<FactorInversionExploratorio> result = em
                    .createNativeQuery(queryString, FactorInversionExploratorio.class)
                    .setParameter("idOportunidad", idOportunidad)
                    .getResultStream()
                    .findFirst();

            if (result.isEmpty()) {
                Log.error("FactorInversionExploratorio: Value not present with idOportunidad = " + idOportunidad);
                throw new SqlExecutionErrorException(
                        "FactorInversionExploratorio: Value not present with idOportunidad = " + idOportunidad);
            }

            return result.get();
        } catch (Exception e) {
            Log.error("JDBC: getFactorInversionExploratorio exception executing SQL", e);
            throw new SqlExecutionErrorException("JDBC getFactorInversionExploratorio exception executing SQL");
        }
    }

    @Transactional
    @CacheResult(cacheName = "factor-inversion-desarrollo-cache")
    public FactorInversionDesarrollo getFactorInversionDesarrollo(@CacheKey Integer idOportunidad) {

        try {
            final var queryString = """
                    select *
                    from inversion.desarrollooportunidadvw
                    where idoportunidadobjetivo = :idOportunidad
                    and idtipovalor=2
                    """;
            Optional<FactorInversionDesarrollo> result = em
                    .createNativeQuery(queryString, FactorInversionDesarrollo.class)
                    .setParameter("idOportunidad", idOportunidad).getResultStream().findFirst();

            if (result.isEmpty()) {
                Log.error("FactorInversionDesarrollo: Value no present with idOportunidad = " + idOportunidad);
                throw new SqlExecutionErrorException(
                        "FactorInversionDesarrollo: Value no present with idOportunidad = " + idOportunidad);
            }

            return result.get();

        } catch (Exception e) {
            Log.error("JDBC: getFactorInversionDesarrollo exception executing SQL", e);
            throw new SqlExecutionErrorException("JDBC getFactorInversionDesarrollo exception executing SQL");
        }

    }

    public ProduccionTotalMmbpce getProduccionTotalMmbpce(
            Integer idOportunidad,
            Integer version,
            Double pncuota,
            Double pndeclinada,
            Double pnpce,
            Double pnarea) {
        try {

            List<EscaleraProduccion> results = escaleraProduccionService.calculateEscaleraProduccion(
                    version,
                    idOportunidad,
                    pncuota,
                    pndeclinada,
                    pnpce,
                    pnarea);

            // Calculate the sum of vproduccion
            double total = results.stream()
                    .mapToDouble(EscaleraProduccion::getProduccion)
                    .sum();

            // Create and return ProduccionTotalMmbpce object
            ProduccionTotalMmbpce produccionTotal = new ProduccionTotalMmbpce();
            produccionTotal.setProduccionTotalMmbpce(total);

            if (produccionTotal == null) {
                Log.error("JDBC exception: getProduccionTotalMmbpce Value no present with idOportunidad = "
                        + idOportunidad);
                throw new SqlExecutionErrorException(
                        "JDBC exception: getProduccionTotalMmbpce Value no present with idOportunidad = "
                                + idOportunidad);
            }

            return produccionTotal;

        } catch (Exception e) {
            Log.error("JDBC: getProduccionTotalMmbpce exception executing SQL", e);
            throw new SqlExecutionErrorException("JDBC getProduccionTotalMmbpce exception executing SQL");
        }
    }

    @Transactional
    @CacheResult(cacheName = "informacion-inversion-cache")
    public InformacionInversion getInformacionInversion(@CacheKey Integer idOportunidad) {

        try {
            final var queryString = """
                    select *
                    from inversion.otrosdatosvw
                    where idoportunidadobjetivo = :idOportunidad
                    and idtipovalor=2
                    """;
            Optional<InformacionInversion> result = em.createNativeQuery(queryString, InformacionInversion.class)
                    .setParameter("idOportunidad", idOportunidad).getResultStream().findFirst();

            if (result.isEmpty()) {
                Log.error("JDBC exception: getInformacionInversion Value no present with idOportunidad = "
                        + idOportunidad);
                throw new SqlExecutionErrorException(
                        "JDBC exception: getInformacionInversion Value no present with idOportunidad = "
                                + idOportunidad);
            }
            return result.get();
        } catch (Exception e) {
            Log.error("JDBC getInformacionInversion exception executing SQL", e);
            throw new SqlExecutionErrorException("JDBC getInformacionInversion exception executing SQL");
        }

    }

    @Transactional
    @CacheResult(cacheName = "factor-inversion-cache")
    public FactorInversion getFactorInversion(@CacheKey Integer idOportunidad) {

        try {
            final var queryString = """
                    SELECT
                        DISTINCT
                        ro.idhidrocarburo,
                        mv.idoportunidadobjetivo,
                        h.hidrocarburo,
                        COALESCE(mv.mediapce,0),
                        COALESCE(mv.mediaaceite/mv.mediapce,0 )AS fc_aceite,
                        COALESCE(mv.mediagas/mv.mediapce,0) AS fc_gas,
                        COALESCE(mv.mediacondensado/mv.mediapce,0) AS fc_condensado
                        FROM catalogo.mediavolumetriaoportunidadtbl mv
                    INNER JOIN catalogo.reloportunidadobjetivotbl ro on mv.idoportunidadobjetivo = ro.idoportunidadobjetivo
                    INNER JOIN catalogo.hidrocarburotbl h on ro.idhidrocarburo = h.idhidrocarburo
                    WHERE mv.idoportunidadobjetivo =:idOportunidad;
                    """;

            Optional<FactorInversion> result = em.createNativeQuery(queryString, FactorInversion.class)
                    .setParameter("idOportunidad", idOportunidad)
                    .getResultStream().findFirst();
            if (result.isEmpty()) {
                Log.error("FactorInversion: Value no present with idOportunidad = " + idOportunidad);
                throw new SqlExecutionErrorException(
                        "FactorInversion: Value no present with idOportunidad = " + idOportunidad);
            }
            return result.get();
        } catch (Exception e) {
            Log.error("JDBC: getFactorInversion exception executing SQL", e);
            throw new SqlExecutionErrorException("JDBC getInformacionInversion exception executing SQL");
        }

    }

    @Transactional
    @CacheResult(cacheName = "costo-operacion-cache")
    public List<CostoOperacion> getCostoOperacion(@CacheKey Integer idProyecto) {

        try {
            final var queryString = """
                    SELECT idcostooperacion, costooperacion, fecha, gasto
                    FROM costo.operacionproyectovw
                    WHERE idproyecto = :idProyecto
                    AND idtipovalor=2
                    """;
            List<CostoOperacion> result = em.createNativeQuery(queryString, CostoOperacion.class)
                    .setParameter("idProyecto", idProyecto).getResultStream().toList();
            return result;
        } catch (Exception e) {
            Log.error("JDBC: getCostoOperacion exception executing SQL", e);
            throw new SqlExecutionErrorException("JDBC getCostoOperacion exception executing SQL");
        }

    }

    @Transactional
    @CacheResult(cacheName = "oportunidades-by-version-cache")
    public List<Oportunidades> getOportunidadesByNombreVersion(@CacheKey String nombreVersion) {

        try {
            final var queryString = """
                    SELECT idoportunidadobjetivo, idoportunidad, oportunidad, b.nombreversion FROM catalogo.claveobjetivovw a
                    JOIN catalogo.versiontbl b ON a.idversion = b.idversion
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

    @Transactional
    @CacheResult(cacheName = "info-oportunidad-cache")
    public InformacionOportunidad getInfoOportunidad(@CacheKey Integer idOportunidad) {
        return InformacionOportunidad.findByIdoportunidadobjetivo(idOportunidad);
    }

    // 1 -> Derecho Extraccion de Hidrocarburos
    @Transactional
    @CacheResult(cacheName = "get-deh-cache")
    public List<Map<String, Object>> getDEH(@CacheKey Integer idOportunidadObj, @CacheKey Integer idVersion) {
        try {
            final var queryString = """
                        select * from impuesto.spc_dehoportunidad(:idVersion, :idOportunidadObj)
                    """;

            List<Object[]> result = em.createNativeQuery(queryString)
                    .setParameter("idVersion", idVersion)
                    .setParameter("idOportunidadObj", idOportunidadObj).getResultList();

            // List<Object[]> result = em.createNativeQuery(queryString).getResultList();
            for (Object[] row : result) {
                System.out.println(row[0] + " " + row[1] + " " + row[2]);
            }
            // lo paso a un formato para el excel
            List<Map<String, Object>> formattedResult = new ArrayList<>();
            for (Object[] row : result) {
                Map<String, Object> map = new HashMap<>();
                map.put("anio", row[0]);
                map.put("tipo", row[1]);
                map.put("duh", row[2]);
                formattedResult.add(map);
            }
            return formattedResult;

        } catch (Exception e) {
            Log.error("JDBC: getImpuestos exception executing SQL", e);
            throw new SqlExecutionErrorException("JDBC getImpuestos exception executing SQL");
        }
    }

    // 2 -> Derecho de Utilidad Compartida
    @Transactional
    @CacheResult(cacheName = "get-deh-cache")
    public List<Map<String, Object>> getDUC(@CacheKey Integer idOportunidadObj, @CacheKey Integer idVersion) {
        try {
            final var queryString = """
                        select
                        	sanio,
                        	 vduc1,vduc2,vduc3,
                        	 vderecho
                        from impuesto.spc_ducoportunidad(:idVersion, :idOportunidadObj)
                    """;

            List<Object[]> result = em.createNativeQuery(queryString).setParameter("idVersion", idVersion)
                    .setParameter("idOportunidadObj", idOportunidadObj).getResultList();

            for (Object[] row : result) {
                System.out.println(row[0] + " " + row[1] + " " + row[2] + " " + row[3] + " " + row[4]);
            }

            // List<Object[]> result = em.createNativeQuery(queryString).getResultList();
            // lo paso a un formato para el excel
            List<Map<String, Object>> formattedResult = new ArrayList<>();
            for (Object[] row : result) {
                Map<String, Object> map = new HashMap<>();
                map.put("anio", row[0]);
                map.put("vduc1", row[1]);
                map.put("vduc2", row[2]);
                map.put("vduc3", row[3]);
                map.put("vderecho", row[4]);
                formattedResult.add(map);
            }
            return formattedResult;

        } catch (Exception e) {
            Log.error("JDBC: getImpuestos exception executing SQL", e);
            throw new SqlExecutionErrorException("JDBC getImpuestos exception executing SQL");
        }
    }

    // 3 -> Impuesto Uso Superficial
    @Transactional
    @CacheResult(cacheName = "get-ius-cache")
    public List<Map<String, Object>> getIUS(@CacheKey Integer idOportunidadObj, @CacheKey Integer idVersion) {
        try {
            final var queryString = """
                        select
                        ianio, iius from impuesto.spc_iusoportunidad(:idVersion, :idOportunidadObj)
                    """;

            List<Object[]> result = em.createNativeQuery(queryString)
                    .setParameter("idVersion", idVersion)
                    .setParameter("idOportunidadObj", idOportunidadObj).getResultList();

            // List<Object[]> result = em.createNativeQuery(queryString).getResultList();
            for (Object[] row : result) {
                System.out.println(row[0] + " " + row[1]);
            }
            // lo paso a un formato para el excel
            List<Map<String, Object>> formattedResult = new ArrayList<>();
            for (Object[] row : result) {
                Map<String, Object> map = new HashMap<>();
                map.put("anio", row[0]);
                map.put("imp", row[1]);

                formattedResult.add(map);
            }
            return formattedResult;

        } catch (Exception e) {
            Log.error("JDBC: getImpuestos exception executing SQL", e);
            throw new SqlExecutionErrorException("JDBC getImpuestos exception executing SQL");
        }
    }

    // 4 -> Cuota Contractual para la fase exploratoria
    @Transactional
    @CacheResult(cacheName = "get-cco-cache")
    public List<Map<String, Object>> getCCO(@CacheKey Integer idOportunidadObj, @CacheKey Integer idVersion) {
        try {
            final var queryString = """
                        select
                        	canio,
                        	ccuotaexploratoria
                        from impuesto.spc_cuotacontoportunidad(:idVersion,:idOportunidadObj)
                    """;

            List<Object[]> result = em.createNativeQuery(queryString)
                    .setParameter("idVersion", idVersion)
                    .setParameter("idOportunidadObj", idOportunidadObj).getResultList();

            // List<Object[]> result = em.createNativeQuery(queryString).getResultList();
            for (Object[] row : result) {
                System.out.println(row[0] + " " + row[1]);
            }
            // lo paso a un formato para el excel
            List<Map<String, Object>> formattedResult = new ArrayList<>();
            for (Object[] row : result) {
                Map<String, Object> map = new HashMap<>();
                map.put("anio", row[0]);
                map.put("imp", row[1]);
                formattedResult.add(map);
            }
            return formattedResult;
        } catch (Exception e) {
            Log.error("JDBC: getImpuestos exception executing SQL", e);
            throw new SqlExecutionErrorException("JDBC getImpuestos exception executing SQL");
        }
    }

    // 5 -> Impuesto actividad de exploracion y extraccion
    @Transactional
    @CacheResult(cacheName = "get-ieo-cache")
    public List<Map<String, Object>> getIEO(@CacheKey Integer idOportunidadObj, @CacheKey Integer idVersion) {
        try {
            final var queryString = """

                        select aanio,
                        aimpactividad
                        from impuesto.spc_impactexplorontoportunidad(:idVersion, :idOportunidadObj)


                    """;

            List<Object[]> result = em.createNativeQuery(queryString)
                    .setParameter("idVersion", idVersion)
                    .setParameter("idOportunidadObj", idOportunidadObj).getResultList();

            // List<Object[]> result = em.createNativeQuery(queryString).getResultList();
            for (Object[] row : result) {
                System.out.println(row[0] + " " + row[1]);
            }
            // lo paso a un formato para el excel
            List<Map<String, Object>> formattedResult = new ArrayList<>();
            for (Object[] row : result) {
                Map<String, Object> map = new HashMap<>();
                map.put("anio", row[0]);
                map.put("imp", row[1]);
                formattedResult.add(map);
            }
            return formattedResult;
        } catch (Exception e) {
            Log.error("JDBC: getImpuestos exception executing SQL", e);
            throw new SqlExecutionErrorException("JDBC getImpuestos exception executing SQL");
        }
    }

    // 6 -> Impuesto sobre la renta
    @Transactional
    @CacheResult(cacheName = "get-isr-cache")
    public List<Map<String, Object>> getISR(@CacheKey Integer idOportunidadObj, @CacheKey Integer idVersion) {
        try {
            final var queryString = """
                       select
                          imanio,
                          vdeduccion10,
                          vdeduccion25,
                          impdepreciacion,
                          vcosto,
                          visr from impuesto.spc_isroportunidad(:idVersion,:idOportunidadObj);
                    """;

            List<Object[]> result = em.createNativeQuery(queryString)
                    .setParameter("idVersion", idVersion)
                    .setParameter("idOportunidadObj", idOportunidadObj).getResultList();

            // List<Object[]> result = em.createNativeQuery(queryString).getResultList();
            for (Object[] row : result) {
                System.out.println(row[0] + " " + row[1] + " " + row[2] + " " + row[3] + " " + row[4] + " " + row[5]);
            }
            // lo paso a un formato para el excel
            List<Map<String, Object>> formattedResult = new ArrayList<>();
            for (Object[] row : result) {
                Map<String, Object> map = new HashMap<>();
                map.put("anio", row[0]);
                map.put("vdeduccion10", row[1]);
                map.put("vdeduccion25", row[2]);
                map.put("impdepreciacion", row[3]);
                map.put("vcosto", row[4]);
                map.put("visr", row[5]);
                formattedResult.add(map);
            }
            return formattedResult;
        } catch (Exception e) {
            Log.error("JDBC: getImpuestos exception executing SQL", e);
            throw new SqlExecutionErrorException("JDBC getImpuestos exception executing SQL");
        }
    }

}
