# NYC Taxi Medallion Data Pipeline

**Estudiante:** Pablo Alvarado  
**Código:** 00344965

Proyecto universitario de Data Mining / Data Engineering orientado a construir un pipeline Medallion completo sobre el dataset oficial **NYC TLC Trip Record Data**, integrando ingesta en Bronze, limpieza en Silver, modelado analítico en Gold y orquestación centralizada con Mage.

Stack tecnológico:

- Docker
- Docker Compose
- PostgreSQL
- pgAdmin
- Mage
- dbt-postgres
- Python
- SQL

Fuente de datos:

- NYC TLC Trip Record Data  
  https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page

Cobertura del proyecto:

- `yellow taxis`
- `green taxis`
- `taxi_zone_lookup`

## 1. Arquitectura Bronze Silver Gold

El proyecto implementa una arquitectura Medallion de tres capas:

**Bronze**

- Ingesta raw en PostgreSQL usando Mage.
- Carga mensual por servicio.
- Tablas principales:
  - `bronze.yellow_trips_raw`
  - `bronze.green_trips_raw`
  - `bronze.taxi_zone_lookup`
  - `bronze.ingest_audit`
- Metadatos de ingesta por fila:
  - `ingest_ts`
  - `source_month`
  - `service_type`
- Estrategia idempotente por mes y servicio mediante `DELETE` previo antes de insertar.

**Silver**

- Transformación, tipificación y estandarización con dbt.
- Modelos:
  - `silver_yellow_trips`
  - `silver_green_trips`
  - `silver_zones`
  - `silver_trips_unified`
- Materialización: `views`.
- Reglas de limpieza aplicadas:
  - pickup no nulo
  - dropoff no nulo
  - pickup <= dropoff
  - `trip_distance >= 0`
  - `total_amount >= 0`

**Gold**

- Modelo dimensional en estrella para analítica.
- Modelos:
  - `dim_date`
  - `dim_zone`
  - `dim_service_type`
  - `dim_payment_type`
  - `fct_trips`
- Materialización: `tables`.
- Granularidad de la tabla de hechos:
  - `1 fila = 1 viaje`

## 2. Diagrama textual del flujo

```text
NYC TLC Trip Record Data (Yellow + Green parquet mensual)
        + Taxi Zone Lookup CSV
                      |
                      v
                Mage: ingest_bronze
                      |
                      v
    +-------------------------------------------------+
    | BRONZE (PostgreSQL)                             |
    | bronze.yellow_trips_raw                         |
    | bronze.green_trips_raw                          |
    | bronze.taxi_zone_lookup                         |
    | bronze.ingest_audit                             |
    +-------------------------------------------------+
                      |
                      v
          Sensor: check_bronze_pipeline
                      |
                      v
             Mage + dbt: dbt_build_silver
                      |
                      v
    +-------------------------------------------------+
    | SILVER (dbt views)                              |
    | silver_yellow_trips                             |
    | silver_green_trips                              |
    | silver_zones                                    |
    | silver_trips_unified                            |
    +-------------------------------------------------+
                      |
                      v
          Sensor: check_silver_pipeline
                      |
                      v
              Etapa Gold en Mage                      |
              1. Script SQL de particionamiento       |
              2. Build de modelos Gold con dbt        |
              3. Validación de calidad                |
                      |
                      v
    +-------------------------------------------------+
    | GOLD (PostgreSQL)                               |
    | gold.dim_date                                   |
    | gold.dim_zone                                   |
    | gold.dim_service_type                           |
    | gold.dim_payment_type                           |
    | gold.fct_trips                                  |
    +-------------------------------------------------+
```

## 3. Tabla de cobertura por mes y servicio

La cobertura mensual se audita en `bronze.ingest_audit` y se encuentra respaldada por los logs versionados de Bronze.

| year_month | service_type | status | row_count |
|---|---|---|---:|
| 2022-01 | green | loaded | 62,495 |
| 2022-01 | yellow | loaded | 2,463,931 |
| 2022-02 | green | loaded | 69,399 |
| 2022-02 | yellow | loaded | 2,979,431 |
| 2022-03 | green | loaded | 78,537 |
| 2022-03 | yellow | loaded | 3,627,882 |
| 2022-04 | green | loaded | 76,136 |
| 2022-04 | yellow | loaded | 3,599,920 |
| 2022-05 | green | loaded | 76,891 |
| 2022-05 | yellow | loaded | 3,588,295 |
| 2022-06 | green | loaded | 73,718 |
| 2022-06 | yellow | loaded | 3,558,124 |
| 2022-07 | green | loaded | 64,192 |
| 2022-07 | yellow | loaded | 3,174,394 |
| 2022-08 | green | loaded | 65,929 |
| 2022-08 | yellow | loaded | 3,152,677 |
| 2022-09 | green | loaded | 69,031 |
| 2022-09 | yellow | loaded | 3,183,767 |
| 2022-10 | green | loaded | 69,322 |
| 2022-10 | yellow | loaded | 3,675,411 |
| 2022-11 | green | loaded | 62,313 |
| 2022-11 | yellow | loaded | 3,252,717 |
| 2022-12 | green | loaded | 72,439 |
| 2022-12 | yellow | loaded | 3,399,549 |
| 2023-01 | green | loaded | 68,211 |
| 2023-01 | yellow | loaded | 3,066,766 |
| 2023-02 | green | loaded | 64,809 |
| 2023-02 | yellow | loaded | 2,913,955 |
| 2023-03 | green | loaded | 72,044 |
| 2023-03 | yellow | loaded | 3,403,766 |
| 2023-04 | green | loaded | 65,392 |
| 2023-04 | yellow | loaded | 3,288,250 |
| 2023-05 | green | loaded | 69,174 |
| 2023-05 | yellow | loaded | 3,513,649 |
| 2023-06 | green | loaded | 65,550 |
| 2023-06 | yellow | loaded | 3,307,234 |
| 2023-07 | green | loaded | 61,343 |
| 2023-07 | yellow | loaded | 2,907,108 |
| 2023-08 | green | loaded | 60,649 |
| 2023-08 | yellow | loaded | 2,824,209 |
| 2023-09 | green | loaded | 65,471 |
| 2023-09 | yellow | loaded | 2,846,722 |
| 2023-10 | green | loaded | 66,177 |
| 2023-10 | yellow | loaded | 3,522,285 |
| 2023-11 | green | loaded | 64,025 |
| 2023-11 | yellow | loaded | 3,339,715 |
| 2023-12 | green | loaded | 64,215 |
| 2023-12 | yellow | loaded | 3,376,567 |
| 2024-01 | green | loaded | 56,551 |
| 2024-01 | yellow | loaded | 2,964,624 |
| 2024-02 | green | loaded | 53,577 |
| 2024-02 | yellow | loaded | 3,007,526 |
| 2024-03 | green | loaded | 57,457 |
| 2024-03 | yellow | loaded | 3,582,628 |
| 2024-04 | green | loaded | 56,471 |
| 2024-04 | yellow | loaded | 3,514,289 |
| 2024-05 | green | loaded | 61,003 |
| 2024-05 | yellow | loaded | 3,723,833 |
| 2024-06 | green | loaded | 54,748 |
| 2024-06 | yellow | loaded | 3,539,193 |
| 2024-07 | green | loaded | 51,837 |
| 2024-07 | yellow | loaded | 3,076,903 |
| 2024-08 | green | loaded | 51,771 |
| 2024-08 | yellow | loaded | 2,979,183 |
| 2024-09 | green | loaded | 54,440 |
| 2024-09 | yellow | loaded | 3,633,030 |
| 2024-10 | green | loaded | 56,147 |
| 2024-10 | yellow | loaded | 3,833,771 |
| 2024-11 | green | loaded | 52,222 |
| 2024-11 | yellow | loaded | 3,646,369 |
| 2024-12 | green | loaded | 53,994 |
| 2024-12 | yellow | loaded | 3,668,371 |
| 2025-01 | green | loaded | 48,326 |
| 2025-01 | yellow | loaded | 3,475,226 |
| 2025-02 | green | loaded | 46,621 |
| 2025-02 | yellow | loaded | 3,577,543 |
| 2025-03 | green | loaded | 51,539 |
| 2025-03 | yellow | loaded | 4,145,257 |
| 2025-04 | green | loaded | 52,132 |
| 2025-04 | yellow | loaded | 3,970,553 |
| 2025-05 | green | loaded | 55,399 |
| 2025-05 | yellow | loaded | 4,591,845 |
| 2025-06 | green | loaded | 49,390 |
| 2025-06 | yellow | loaded | 4,322,960 |
| 2025-07 | green | loaded | 48,205 |
| 2025-07 | yellow | loaded | 3,898,963 |
| 2025-08 | green | loaded | 46,306 |
| 2025-08 | yellow | loaded | 3,574,091 |
| 2025-09 | green | loaded | 48,893 |
| 2025-09 | yellow | loaded | 4,251,015 |
| 2025-10 | green | loaded | 49,416 |
| 2025-10 | yellow | loaded | 4,428,699 |
| 2025-11 | green | loaded | 46,912 |
| 2025-11 | yellow | loaded | 4,181,444 |

Consulta recomendada de auditoría:

```sql
SELECT
    year_month,
    service_type,
    status,
    row_count,
    ingest_ts
FROM bronze.ingest_audit
ORDER BY year_month, service_type;
```

## 4. Cómo levantar el stack con Docker

**Prerrequisitos**

- Docker
- Docker Compose

**Inicialización**

```bash
cp .env.example .env
docker compose up --build -d
docker compose ps
```

**Servicios expuestos**

- Mage: `http://localhost:6789`
- pgAdmin: `http://localhost:5050`
- PostgreSQL: puerto definido en `POSTGRES_PORT`

**Apagado**

```bash
docker compose down
```

**Reset completo**

```bash
docker compose down -v
```

## 5. Pipelines de Mage y qué hace cada uno

### `ingest_bronze`

- Inicializa objetos Bronze.
- Descarga `taxi_zone_lookup`.
- Descarga archivos mensuales `yellow` y `green`.
- Normaliza columnas y tipos.
- Inserta metadatos de ingesta.
- Ejecuta carga idempotente por `source_month` y `service_type`.
- Registra cobertura y auditoría en `bronze.ingest_audit`.

### `dbt_build_silver`

- Espera la finalización de Bronze mediante sensor.
- Ejecuta los modelos Silver:
  - `silver_green_trips`
  - `silver_yellow_trips`
  - `silver_zones`
  - `silver_trips_unified`
- Construye la capa curada para estandarización y unificación.

### Etapa Gold

La etapa Gold se organiza en este orden:

1. ejecución del script SQL de particionamiento
2. construcción de modelos Gold con dbt
3. validación de calidad

Script SQL Gold:

- `mage/mage_project/sql/gold_partitioning.sql`

Modelos Gold:

- `dim_zone`
- `dim_service_type`
- `dim_payment_type`
- `dim_date`
- `fct_trips`

## 6. Triggers y sensores

### Schedules configurados

- `start_bronze_ingest`
  - tipo: `TIME`
  - pipeline: `ingest_bronze`
  - intervalo: `@once`
- `start_silver_version`
  - tipo: `TIME`
  - pipeline: `dbt_build_silver`
  - intervalo: `@hourly`
- `start_gold`
  - tipo: `TIME`
  - pipeline: `db_build_gold`
  - intervalo: `@hourly`

### Sensores implementados

- `check_bronze_pipeline`
  - habilita la ejecución de Silver cuando Bronze ha concluido correctamente.
- `check_silver_pipeline`
  - habilita la ejecución de Gold cuando Silver ha concluido correctamente.

### Secuencia operativa

```text
start_bronze_ingest
    -> ingest_bronze
    -> check_bronze_pipeline
    -> dbt_build_silver
    -> check_silver_pipeline
    -> ejecución de particionamiento Gold
    -> dbt Gold
    -> quality checks
```

## 7. Gestión de Secrets y `.env.example`

La configuración sensible del proyecto se externaliza mediante variables de entorno para evitar credenciales hardcodeadas en código o en modelos dbt.

Variables principales:

- `POSTGRES_DB`
- `POSTGRES_HOST`
- `POSTGRES_USER`
- `POSTGRES_PASSWORD`
- `POSTGRES_PORT`
- `NYC_TLC_BASE_URL`
- `NYC_TLC_ZONE_URL`
- `INGEST_START_MONTH`
- `INGEST_END_MONTH`
- `DBT_TIMEOUT_SECONDS`

Buenas prácticas aplicadas:

- `.env` se mantiene fuera del versionamiento.
- `.env.example` se encuentra versionado como plantilla.
- `profiles.yml` de dbt consume credenciales con `env_var(...)`.
- La misma convención de nombres puede registrarse en Mage para ejecución centralizada del pipeline.

## 8. Explicación del particionamiento

El particionamiento forma parte de la etapa Gold y está centralizado en:

- `mage/mage_project/sql/gold_partitioning.sql`

El script crea el esquema `gold` y define las tablas particionadas necesarias antes del build analítico.

### Tipos de partición implementados

**`gold.fct_trips`**

- Tipo: `RANGE`
- Clave: `pickup_date`
- Cobertura de particiones:
  - mensual
  - desde `2024-01` hasta `2025-12`
- La columna `pickup_date` se genera a partir de `pickup_datetime`.

**`gold.dim_zone`**

- Tipo: `HASH`
- Clave: `zone_id`
- Número de particiones: `4`

**`gold.dim_service_type`**

- Tipo: `LIST`
- Clave: `service_type`
- Particiones:
  - `yellow`
  - `green`

**`gold.dim_payment_type`**

- Tipo: `LIST`
- Clave: `payment_type_desc`
- Particiones:
  - `Cash`
  - `Credit card`
  - `Other`, `Unknown`, `No charge`, `Dispute`, `Voided trip`

### Ejecución recomendada dentro de Mage

Antes del build Gold, ejecutar el script:

```bash
docker compose exec mage bash
psql "postgresql://${POSTGRES_USER}:${POSTGRES_PASSWORD}@${POSTGRES_HOST}:${POSTGRES_PORT}/${POSTGRES_DB}" \
  -f /home/src/mage_project/sql/gold_partitioning.sql
```

## 9. Evidencias de particiones (`\d+`)

La validación estructural del particionamiento se realiza con:

```sql
\d+ gold.fct_trips
\d+ gold.dim_zone
\d+ gold.dim_service_type
\d+ gold.dim_payment_type
```

Qué debe observarse:

- `gold.fct_trips`
  - `Partition key: RANGE (pickup_date)`
- `gold.dim_zone`
  - `Partition key: HASH (zone_id)`
- `gold.dim_service_type`
  - `Partition key: LIST (service_type)`
- `gold.dim_payment_type`
  - `Partition key: LIST (payment_type_desc)`

Consulta adicional para listar particiones creadas:

```sql
SELECT
    parent.relname AS parent_table,
    child.relname AS child_partition
FROM pg_inherits i
JOIN pg_class parent ON parent.oid = i.inhparent
JOIN pg_class child ON child.oid = i.inhrelid
JOIN pg_namespace n ON n.oid = parent.relnamespace
WHERE n.nspname = 'gold'
ORDER BY parent.relname, child.relname;
```

## 10. Evidencias de partition pruning (EXPLAIN)

### Consulta 1: filtro por mes sobre `fct_trips`

```sql
EXPLAIN (ANALYZE, BUFFERS)
SELECT COUNT(*)
FROM gold.fct_trips
WHERE pickup_date >= DATE '2024-01-01'
  AND pickup_date < DATE '2024-02-01';
```

Interpretación esperada:

- el plan debe acceder únicamente a la partición del mes filtrado;
- la evidencia de pruning se observa cuando el plan no recorre todas las particiones hijas.

### Consulta 2: búsqueda puntual sobre `dim_zone`

```sql
EXPLAIN (ANALYZE, BUFFERS)
SELECT *
FROM gold.dim_zone
WHERE zone_id = 132;
```

Interpretación esperada:

- el optimizador debe resolver el acceso sobre una sola partición hash;
- el plan debe mostrar restricción de acceso al bucket correspondiente.

## 11. Evidencias de dbt run y dbt test

### Materializations

- Silver: `view`
- Gold: `table`

### Logs de referencia

**Silver**

- `mage/mage_project/pipelines/dbt_build_silver/.logs/2/20260311T040500/dbt_nyc_dbt_project_models_silver_silver_yellow_trips.log`

Evidencia observada en logs:

- ejecución de `dbt build --select silver_yellow_trips`
- creación del modelo Silver
- resultado:
  - `Done. PASS=1 WARN=0 ERROR=0 SKIP=0 TOTAL=1`

**Gold**

- `mage/mage_project/pipelines/db_build_gold/.logs/3/20260311T050000/dbt_nyc_dbt_project_models_gold_dim_payment_type.log`

Evidencia observada en logs:

- ejecución de `dbt build --select dim_payment_type`
- construcción del modelo Gold
- ejecución de tests `not_null` y `unique`
- resultado:
  - `Done. PASS=3 WARN=0 ERROR=0 SKIP=0 TOTAL=3`

**Fact table**

- `mage/mage_project/pipelines/db_build_gold/.logs/3/20260309T053500/dbt_nyc_dbt_project_models_gold_fct_trips.log`

Evidencia observada:

- creación de `fct_trips`
- resultado:
  - `Done. PASS=1 WARN=0 ERROR=0 SKIP=0 TOTAL=1`

## 12. Evidencias de ejecución del pipeline

Capturas incluidas en el repositorio:

- `evidencias/evidenciaPipelines.png`
- `evidencias/bronze_pipeline.png`
- `evidencias/evidenciaPipeline_Bronze_Start.png`
- `evidencias/evidenciaPipeline_Bronze_End.png`
- `evidencias/evidenciaPipeline_Bronze_AuditCargados.png`
- `evidencias/silver_pipeline.png`
- `evidencias/evidenciaPipeline_Silver.png`
- `evidencias/gold_pipeline.png`
- `evidencias/total_ingested_data.png`
- `evidencias/evidenciaVolume_Size.png`

Estas evidencias documentan:

- organización de pipelines en Mage
- inicio y fin de Bronze
- auditoría de carga
- ejecución de Silver
- ejecución de Gold
- volumen persistente de Docker

## 13. Troubleshooting

### 1. Docker o servicios no levantan correctamente

Validar:

```bash
docker compose up --build -d
docker compose ps
```

Revisar:

- estado del daemon Docker
- puertos ocupados
- valores de `.env`

### 2. dbt no conecta a PostgreSQL

Validar:

- `POSTGRES_HOST`
- `POSTGRES_PORT`
- `POSTGRES_DB`
- `POSTGRES_USER`
- `POSTGRES_PASSWORD`

Revisar:

- `mage/mage_project/dbt/nyc_dbt_project/profiles.yml`
- interpolación correcta de variables

### 3. Gold no queda particionado

Validar:

- ejecución previa de `mage/mage_project/sql/gold_partitioning.sql`
- creación de esquema `gold`
- salida de `\d+` sobre tablas Gold

### 4. Cobertura incompleta o diferencias de conteo

Validar:

```sql
SELECT *
FROM bronze.ingest_audit
ORDER BY year_month, service_type;
```

Revisar:

- disponibilidad mensual del dataset oficial
- URLs de origen
- logs del pipeline Bronze

## 14. Checklist final del deber

- Arquitectura Medallion implementada con capas Bronze, Silver y Gold.
- Ingesta mensual versionada para `yellow`, `green` y `taxi_zone_lookup`.
- Idempotencia documentada mediante borrado previo por mes y servicio.
- Auditoría de cobertura por `year_month` y `service_type`.
- Silver materializado como vistas.
- Gold materializado como tablas.
- Modelo estrella con una tabla de hechos y dimensiones analíticas.
- Particionamiento Gold documentado y centralizado en `mage/mage_project/sql/gold_partitioning.sql`.
- Validación estructural de particiones mediante `\d+`.
- Validación de pruning mediante `EXPLAIN (ANALYZE, BUFFERS)`.
- dbt ejecutado desde Mage.
- Evidencias de ejecución del pipeline incluidas en el repositorio.
- Notebook analítico basado en `gold.*`.
