# Pset 2
Juan Pablo Bautista 
00328922

## Descripción

En este proyecto construí un **pipeline de datos usando Mage, PostgreSQL
y dbt** para trabajar con el dataset **NYC TLC Trip Record Data (Yellow
y Green Taxi)**.

El pipeline implementa una **arquitectura medallion**:

-   **bronze** → datos crudos
-   **silver** → datos limpios y estandarizados
-   **gold** → modelo estrella para análisis

Granularidad final:

    1 fila = 1 viaje

Cobertura de datos:

    2022
    2023
    2024
    2025

------------------------------------------------------------------------

# Arquitectura del pipeline

Flujo general:

    NYC TLC Trip Records
            │
            ▼
    Mage pipeline: ingest_bronze
            │
            ▼
    PostgreSQL bronze (raw tables)
            │
            ▼
    dbt_build_silver
            │
            ▼
    silver views (datos limpios)
            │
            ▼
    dbt_build_gold
            │
            ▼
    gold star schema
            │
            ▼
    quality_checks (dbt test)
            │
            ▼
    Notebook de análisis

------------------------------------------------------------------------

# Cómo ejecutar el proyecto

## 1. Levantar servicios

``` bash
docker compose up --build
```

Servicios levantados:

-   PostgreSQL
-   PgAdmin
-   Mage

------------------------------------------------------------------------

## 2. Crear esquemas en PostgreSQL

Después de levantar los servicios fue necesario crear manualmente los
esquemas:

``` sql
CREATE SCHEMA bronze;
CREATE SCHEMA silver;
CREATE SCHEMA gold;
```

------------------------------------------------------------------------

## 3. Ejecutar dbt manualmente (opcional)

``` bash
dbt run --select silver
dbt run --select gold
dbt test --select silver
dbt test --select gold
```

Normalmente estos comandos se ejecutan dentro de **Mage pipelines**.

------------------------------------------------------------------------

# Herramientas usadas

-   Docker
-   PostgreSQL
-   Mage
-   dbt

------------------------------------------------------------------------

# Capas

## Bronze

En esta capa se cargan los datos mensuales de:

-   Yellow Taxi
-   Green Taxi

Se agregaron metadatos de ingesta:

-   `ingest_ts`
-   `source_month`
-   `service_type`

La carga es **idempotente**, es decir:

Si se vuelve a correr un mes:

1.  se borra ese mes
2.  se vuelve a insertar




------------------------------------------------------------------------

# Tabla de cobertura y auditoria de ingesta

  year_month   service_type   status   row_count
  ------------ -------------- -------- -----------
  2022-01      yellow         loaded   ...
  2022-01      green          loaded   ...
  ...          ...            ...      ...
  2025-12      green          loaded   ...

Esta tabla permite verificar que todos los meses fueron cargados
correctamente. 
para evitar reprocesar datos ya procesados

------------------------------------------------------------------------

# Silver

En esta capa se limpiaron y validaron los datos.

Modelos principales:

-   `silver_taxi_zones`
-   `silver_trips`

Reglas aplicadas:

-   `pickup_ts` no puede ser nulo
-   `dropoff_ts` no puede ser nulo
-   `pickup_ts <= dropoff_ts`
-   `trip_distance >= 0`
-   `total_amount >= 0`
-   validación de zonas usando `taxi_zone_lookup`

Materialización:

    views

------------------------------------------------------------------------

# Gold

En esta capa se construyó el **modelo estrella** para análisis.

Tablas principales:

-   `gold.fct_trips`
-   `gold.dim_date`
-   `gold.dim_zone`
-   `gold.dim_service_type`
-   `gold.dim_payment_type`
-   `gold.dim_vendor`

Materialización:

    tables

------------------------------------------------------------------------

# Particionamiento

Se implementó **particionamiento declarativo en PostgreSQL**.

### Hechos

    gold.fct_trips
    PARTITION BY RANGE (pickup_date)

Particiones mensuales.

------------------------------------------------------------------------

### Dimensiones HASH

    gold.dim_zone
    PARTITION BY HASH (zone_key)

Con **4 particiones**.

------------------------------------------------------------------------

### Dimensiones LIST

    gold.dim_service_type
    PARTITION BY LIST(service_type)

Particiones:

    yellow
    green

    gold.dim_payment_type
    PARTITION BY LIST(payment_type)

Particiones:

    cash
    card
    other

Este particionamiento ayuda a mejorar el rendimiento de consultas.

------------------------------------------------------------------------

# Evidencia de partition pruning

Consulta utilizada:

``` sql
EXPLAIN (ANALYZE, BUFFERS)

SELECT count(*)
FROM gold.fct_trips
WHERE pickup_date BETWEEN '2024-01-01'
AND '2024-01-31';
```

Resultado esperado:

PostgreSQL accede únicamente a la partición:

    fct_trips_2024_01

Esto demuestra **partition pruning**.

------------------------------------------------------------------------

# Pipelines en Mage

Pipelines implementados:

### ingest_bronze

Carga los datos a la capa bronze.

------------------------------------------------------------------------

### dbt_build_silver

Ejecuta:

``` bash
dbt run --select silver
```

------------------------------------------------------------------------

### dbt_build_gold

Ejecuta:

``` bash
dbt run --select gold
```

------------------------------------------------------------------------

### quality_checks

Ejecuta:

``` bash
dbt test --select silver
dbt test --select gold
```

Si un test falla, el pipeline también falla.

------------------------------------------------------------------------

# Triggers

Se configuraron triggers en Mage para automatizar el pipeline.

### ingest_monthly

Tipo:

    schedule trigger

Ejecuta:

    ingest_bronze

------------------------------------------------------------------------

### dbt_after_ingest

Tipo:

    event trigger

Se ejecuta cuando `ingest_bronze` termina exitosamente.

Ejecuta:

    dbt_build_silver
    dbt_build_gold
    quality_checks

------------------------------------------------------------------------

# Seguridad

Las credenciales **no están hardcodeadas**.

Se usaron:

### Mage Secrets

-   `POSTGRES_HOST`
-   `POSTGRES_PORT`
-   `POSTGRES_DB`
-   `POSTGRES_USER`
-   `POSTGRES_PASSWORD`

El archivo `.env` no se incluye en el repositorio.

------------------------------------------------------------------------

# Evidencias incluidas

-   pipelines funcionando en Mage
-   triggers configurados
-   tablas en esquemas `bronze`, `silver` y `gold`
-   particiones creadas en PostgreSQL
-   pruebas de dbt pasando

------------------------------------------------------------------------

# Notebook de análisis

Archivo:

    data_analysis.ipynb

Contiene **20 preguntas de negocio** usando únicamente tablas:

    gold.*

Cada respuesta incluye:

-   query SQL
-   interpretación

-----------------------------------------------------------------------
# Troubleshooting

Error: column does not exist Solución: revisar nombres de columnas.

Error: server closed connection Solución: revisar conexión PostgreSQL.

Error: tipo incorrecto Solución: convertir a date_key.


------------------------------------------------------------------------

# Checklist

-   [x] Docker Compose funcionando
-   [x] Mage conectado a PostgreSQL
-   [x] Secrets y variables de entorno configuradas
-   [x] Pipeline `ingest_bronze` funcionando
-   [x] Carga idempotente por mes
-   [x] Metadatos en bronze (`ingest_ts`, `source_month`,
    `service_type`)
-   [x] Backfill mensual implementado
-   [x] Proyecto dbt conectado correctamente
-   [x] Modelos silver construidos
-   [x] Tests de silver pasando
-   [x] Modelo estrella en gold construido
-   [x] Particionamiento implementado en PostgreSQL
-   [x] Pipelines `dbt_build_silver`, `dbt_build_gold` y
    `quality_checks`
-   [x] Triggers encadenados configurados
-   [x] Notebook con 20 preguntas usando solo `gold.*`

