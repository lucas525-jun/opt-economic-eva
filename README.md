# pemex-ics-o-economic-evaluation

This microservice exposes an endpoint which executes the operations necessary
to return the economic evaluation of an opportunity.

## Endpoints

### 1. [GET] getEvaluacionEconomica

```
This endpoint receives as path param the id of the opportunity and answers 
the economic evaluation of this opportunity.
```

### - Request data

Name          | Type           | Description 
------------- |----------------|----------------------
idOportunidad | integer (path) | Target Opportunity ID

```
http://localhost:8080/api/v1/getEvaluacionEconomica/{idOportunidad}
```

### - Response data

Name                | Type             | Description 
------------------- |------------------|------------------------------
EvaluacionEconomica | application/json | Responds a json object containing the results of the economic evaluation.

```
{
  "infoOportunidad": {
    "idproyecto": 0,
    "proyecto": "string",
    "idoportunidadobjetivo": 0,
    "idoportunidad": 0,
    "oportunidad": "string",
    "idobjetivo": 0,
    "objetivo": "string",
    "fechainicioperfexploratorio": "string",
    "fechainicio": "string",
    "claveoportunidad": "string",
    "claveobjetivo": "string",
    "duracionperfpozodesarrollo": 0,
    "duraciontermpozodesarrollo": 0,
    "idhidrocarburo": 0,
    "hidrocarburo": "string",
    "idtipooportunidad": 0,
    "tipooportunidad": "string",
    "idprograma": 0,
    "programa": "string",
    "idversion": 0
  },
  "evaluacionEconomica": [
    {
      "anio": "string",
      "pozosProduccion": {
        "pozosActivos": 0,
        "pozosPerforados": 0,
        "pozosTerminados": 0
      },
      "produccionDiariaPromedio": {
        "mbpce": 0,
        "aceiteTotal": 0,
        "aceiteExtraPesado": 0,
        "aceitePesado": 0,
        "aceiteLigero": 0,
        "aceiteSuperLigero": 0,
        "gasTotal": 0,
        "gasHumedo": 0,
        "gasSeco": 0,
        "condensado": 0
      },
      "ingresos": {
        "total": 0,
        "aceiteExtraPesado": 0,
        "aceitePesado": 0,
        "aceiteLigero": 0,
        "aceiteSuperLigero": 0,
        "gasHumedo": 0,
        "gasSeco": 0,
        "condensado": 0
      },
      "inversiones": {
        "total": 0,
        "exploratoria": 0,
        "perforacionExp": 0,
        "terminacionExp": 0,
        "infraestructuraExp": 0,
        "desarrolloSinOperacional": 0,
        "perforacionDes": 0,
        "terminacionDes": 0,
        "infraestructuraDes": 0,
        "lineaDescarga": 0,
        "ductos": 0,
        "plataformaDesarrollo": 0,
        "operacionalFuturoDesarrollo": 0,
        "desarrollo": 0
      },
      "costos": {
        "total": 0,
        "fijos": 0,
        "variables": 0,
        "administracionCorporativo": 0,
        "comprasGas": 0,
        "comprasInterorganismos": 0,
        "jubilados": 0,
        "manoObra": 0,
        "materiales": 0,
        "otrosConceptos": 0,
        "reservaLaboral": 0,
        "serviciosCorporativos": 0,
        "serviciosGenerales": 0
      },
      "flujoContable": {
        "egresosTotales": 0,
        "flujosNetosEfectivo": 0,
        "flujosDescontadosEfectivo": 0,
        "flujosDescontadosInversion": 0,
        "flujosDescontadosEgresos": 0,
        "flujosDescontadosIngresos": 0,
        "flujosDescontadosCostos": 0
      }
    }
  ],
  "flujoContableTotal": {
    "vpn": 0,
    "tir": 0,
    "flujosDescontadosEfectivoTotal": 0,
    "valorPresenteInversion": 0,
    "valorPresenteEgresos": 0,
    "valorPresenteIngresos": 0,
    "valorPresenteCostos": 0,
    "reporte708": 0,
    "reporte721": 0,
    "reporte722": 0,
    "reporte723": 0,
    "utilidadBpce": 0,
    "relacionCostoBeneficio": 0,
    "costoDescubrimiento": 0,
    "costoOperacion": 0
  }
}
```

## Running the application in dev mode

You can run your application in dev mode that enables live coding using:

```shell script
./mvnw compile quarkus:dev
```

> **_NOTE:_**  Quarkus now ships with a Dev UI, which is available in dev mode only at <http://localhost:8080/q/dev/>.

## Packaging and running the application

The application can be packaged using:

```shell script
./mvnw package
```

It produces the `quarkus-run.jar` file in the `target/quarkus-app/` directory.
Be aware that it’s not an _über-jar_ as the dependencies are copied into the `target/quarkus-app/lib/` directory.

The application is now runnable using `java -jar target/quarkus-app/quarkus-run.jar`.

If you want to build an _über-jar_, execute the following command:

```shell script
./mvnw package -Dquarkus.package.jar.type=uber-jar
```

The application, packaged as an _über-jar_, is now runnable using `java -jar target/*-runner.jar`.

## Creating a native executable

You can create a native executable using:

```shell script
./mvnw package -Dnative
```

Or, if you don't have GraalVM installed, you can run the native executable build in a container using:

```shell script
./mvnw package -Dnative -Dquarkus.native.container-build=true
```

You can then execute your native executable with: `./target/pemex-ics-o-oportunities-1.0.0-SNAPSHOT-runner`

If you want to learn more about building native executables, please consult <https://quarkus.io/guides/maven-tooling>.
