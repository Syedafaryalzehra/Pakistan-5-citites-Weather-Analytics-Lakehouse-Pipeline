# Pakistan 5-Cities Weather Analytics — Lakehouse Pipeline (Phase 2)

PySpark pipeline on Databricks implementing the **Bronze** and **Silver** layers of a medallion architecture.

- **Source:** Open-Meteo Historical Weather API (hourly data)
- **Cities:** Lahore, Karachi, Islamabad, Peshawar, Quetta
- **Platform:** Databricks (Delta Lake), database `pakistan_weather`

## 1. Architecture

```
Open-Meteo API ──► Raw JSON (Volumes) ──► Bronze (raw, all strings) ──► Silver (typed, deduplicated)
                                              │                              │
                                              └────── pipeline_execution_logs ◄──┘
                                                                        silver_quarantine (rejected rows)
```

| Layer | Table | Behaviour |
|---|---|---|
| Bronze | `bronze_weather_raw` | Raw landing zone. Explicit schema, every value kept as a string. Full load = overwrite (one time), incremental = append. |
| Silver | `silver_weather_cleaned` | One row per city per hour. Types cast, derived columns added, loaded with `MERGE INTO`. |
| Quarantine | `silver_quarantine` | Rows that could not be cast to the expected types. |
| Audit | `pipeline_execution_logs` | One row per layer run. |

## 2. Notebooks

| Notebook | Purpose |
|---|---|
| `01-setup` | Creates the `pakistan_weather` database and the `pipeline_execution_logs` table. |
| `02-bronzelayer` | Raw JSON → Bronze. Reads with an explicit `StructType` (no `inferSchema`). |
| `03-silverlayer` | Bronze → Silver. Explodes hourly arrays, casts types, quarantines bad rows, `MERGE INTO`. |
| `04-incrementalload` | Fetches one day from the API, saves the raw file, then runs `02` and `03`. |

## 3. Data Dictionary

### 3.1 Bronze — `pakistan_weather.bronze_weather_raw`

**Grain:** one row per API response (one city per response).
**Logical key:** `(batch_id, latitude, longitude, hourly.time[0])`. Bronze is append-only, so uniqueness is not enforced here; it is enforced in Silver.

| Column | Type | Description |
|---|---|---|
| `latitude` | STRING | Grid latitude returned by the API |
| `longitude` | STRING | Grid longitude returned by the API |
| `generationtime_ms` | STRING | API generation time |
| `utc_offset_seconds` | STRING | UTC offset |
| `timezone` | STRING | Timezone name (GMT) |
| `timezone_abbreviation` | STRING | Timezone abbreviation |
| `elevation` | STRING | Elevation in metres |
| `hourly` | STRUCT | One `ARRAY<STRING>` per variable (below), aligned by index with `hourly.time` |
| `_rescued_data` | STRING | JSON of any field that did not fit the schema (schema drift capture). NULL when the record matches the schema. |
| `load_timestamp` | TIMESTAMP | When the record was loaded into Bronze |
| `load_type` | STRING | `full` or `incremental` |
| `batch_id` | STRING | Batch identifier |
| `source_file` | STRING | Raw file the record came from |

`hourly` fields (all `ARRAY<STRING>`): `time`, plus the 42 weather variables listed in section 3.2.

### 3.2 Silver — `pakistan_weather.silver_weather_cleaned`

**Grain:** one row per city per hour.
**Primary key:** `(observation_timestamp, latitude, longitude)` — this is the `MERGE INTO` key.

| Column | Type | Description |
|---|---|---|
| `observation_timestamp` | TIMESTAMP | Hour of the observation (parsed from `hourly.time`, GMT) |
| `latitude` | DOUBLE | Latitude, rounded to 4 decimals |
| `longitude` | DOUBLE | Longitude, rounded to 4 decimals |
| `timezone` | STRING | Timezone name |
| `year`, `month`, `day`, `hour` | INT | Derived from `observation_timestamp` |
| `season` | STRING | Spring (Mar–May), Summer (Jun–Aug), Autumn (Sep–Nov), Winter (Dec–Feb) |
| `silver_load_timestamp` | TIMESTAMP | When the record was processed into Silver (this layer's `load_timestamp`) |
| `load_type` | STRING | `full` or `incremental` |
| `batch_id` | STRING | Batch identifier |
| `source_file` | STRING | Raw file the record originated from |

**Measures — BIGINT:** `relative_humidity_2m`, `weather_code`, `wind_direction_10m`, `wind_direction_100m`, `cloud_cover`, `cloud_cover_low`, `cloud_cover_mid`, `cloud_cover_high`

**Measures — DOUBLE:** `temperature_2m`, `dew_point_2m`, `apparent_temperature`, `pressure_msl`, `surface_pressure`, `vapour_pressure_deficit`, `precipitation`, `rain`, `snowfall`, `snow_depth`, `wind_speed_10m`, `wind_speed_100m`, `wind_gusts_10m`, `shortwave_radiation`, `direct_radiation`, `diffuse_radiation`, `direct_normal_irradiance`, `global_tilted_irradiance`, `terrestrial_radiation`, `shortwave_radiation_instant`, `direct_radiation_instant`, `diffuse_radiation_instant`, `direct_normal_irradiance_instant`, `global_tilted_irradiance_instant`, `terrestrial_radiation_instant`, `soil_temperature_0_to_7cm`, `soil_temperature_7_to_28cm`, `soil_temperature_28_to_100cm`, `soil_temperature_100_to_255cm`, `soil_moisture_0_to_7cm`, `soil_moisture_7_to_28cm`, `soil_moisture_28_to_100cm`, `soil_moisture_100_to_255cm`, `et0_fao_evapotranspiration`

### 3.3 Quarantine — `pakistan_weather.silver_quarantine`

**Grain:** one row per rejected hourly record. Cleared and rewritten per `batch_id` on every run, so re-runs do not duplicate rows.

| Column | Type | Description |
|---|---|---|
| `batch_id` | STRING | Batch identifier |
| `load_type` | STRING | `full` or `incremental` |
| `source_file` | STRING | Raw file the record came from |
| `latitude` | DOUBLE | Latitude |
| `longitude` | DOUBLE | Longitude |
| `time_str` | STRING | Raw timestamp string |
| `quarantine_reason` | STRING | `invalid observation_timestamp` or `failed numeric cast` |
| `raw_values` | STRING | JSON of the raw (uncast) values for the rejected record |
| `silver_load_timestamp` | TIMESTAMP | When the record was quarantined |

### 3.4 Audit — `pakistan_weather.pipeline_execution_logs`

**Primary key:** `log_id` (auto-generated identity).

| Column | Type | Description |
|---|---|---|
| `log_id` | BIGINT | Identity key |
| `layer` | STRING | `Raw-to-Bronze` or `Bronze-to-Silver` |
| `load_type` | STRING | `full` or `incremental` |
| `file_processed` | STRING | Raw file path (Bronze) or source table (Silver) |
| `city` | STRING | `All-5-Cities` |
| `start_time` | TIMESTAMP | Run start |
| `end_time` | TIMESTAMP | Run end |
| `status` | STRING | `Success` or `Failure` |
| `rows_inserted` | BIGINT | Rows inserted (Bronze: rows written; Silver: `MERGE` inserts) |
| `rows_updated` | BIGINT | Rows updated (Silver `MERGE` updates) |
| `error_message` | STRING | Error text on failure, otherwise NULL |
| `batch_id` | STRING | Batch identifier |

## 4. Engineering Design

**Strict schema-on-read.** Bronze is read with an explicit `StructType`/`StructField` schema; `inferSchema` is never used. Bronze keeps every value as a string, so type changes in the source do not break ingestion. Types are enforced when data moves to Silver.

**Metadata.** Every Bronze and Silver record carries a load timestamp (`load_timestamp` in Bronze, `silver_load_timestamp` in Silver), plus `load_type`, `batch_id` and `source_file`.

**Idempotency.**
- Bronze is append-only; a full load is a one-time overwrite.
- Silver is loaded with `MERGE INTO` on `(observation_timestamp, latitude, longitude)` after de-duplicating the source. Running the pipeline repeatedly on the same raw data does not add rows to Silver; matched rows are updated in place.
- Quarantine rows are deleted per `batch_id` before being rewritten.

**Schema drift.**

| Drift | Bronze | Silver |
|---|---|---|
| New column in source | Captured in `_rescued_data`; the batch does not fail | Not added automatically; visible in Bronze for later promotion |
| Value cannot be cast to the expected type | Absorbed (all strings) | Row sent to `silver_quarantine` with its raw values; remaining rows load normally |
| Column missing in a batch | NULL in Bronze | Filled with NULL so the `MERGE` still succeeds |

**Audit logging.** Every run of `02` and `03`, full or incremental, writes a row to `pipeline_execution_logs` with layer, file/table processed, start and end time, status and row counts. Failures are logged first and then re-raised, so a failed Bronze run stops `04` before Silver runs.

## 5. Execution Guide

Run `01-setup` once before anything else.

### 5.1 Initial full load (one time)

1. Run `02-bronzelayer` with widgets:
   - `load_type` = `full`
   - `batch_id` = `batch_fullload_2000_2026`
   - `raw_file` = path to the full-load JSON file in Volumes
2. Run `03-silverlayer` with widgets:
   - `load_type` = `full`
   - `batch_id` = `batch_fullload_2000_2026`

### 5.2 Standard incremental load (daily)

Run `04-incrementalload` with no changes. `target_date` defaults to yesterday. The notebook:
1. Fetches that date for all 5 cities and saves `incremental_<date>.json`
2. Runs `02-bronzelayer` (append, `batch_id = batch_incremental_<date>`)
3. Runs `03-silverlayer` (`MERGE` into Silver)

### 5.3 Backfill

| Goal | What to run | Parameters |
|---|---|---|
| Re-load one historical day end to end | `04-incrementalload` | `target_date` = `YYYY-MM-DD` |
| Re-load a range of days | `04-incrementalload` once per date | `target_date` = each date in the range |
| Raw → Bronze only, for any raw file | `02-bronzelayer` | `load_type` = `incremental`, `batch_id`, `raw_file` |
| Bronze → Silver only, for any existing batch | `03-silverlayer` | `load_type`, `batch_id` of the batch to rebuild |

Backfills are safe to repeat: Silver uses `MERGE INTO`, so re-running a date does not create duplicate rows.

> **Note:** if a widget change is ignored when running `02` or `03` standalone, the notebook is still holding the previous run's variables. Detach and re-attach the notebook (or restart the Python session) and run again.

### 5.4 Verifying idempotency

Run the same batch through `03-silverlayer` twice and compare:

```sql
SELECT COUNT(*) FROM pakistan_weather.silver_weather_cleaned;
SELECT layer, status, rows_inserted, rows_updated, batch_id
FROM pakistan_weather.pipeline_execution_logs ORDER BY log_id DESC;
```

The Silver row count must be identical after the second run; the second log row shows rows updated instead of inserted.

## 6. Repository Structure

```
data/
└── full_load/sample
└── incremental_load/sample

phase1/prposal.docx
phase2/
└── notebooks/
    ├── 00-dataaquisition_fullload.ipynb
    ├── 01-setup.ipynb
    ├── 02-bronzelayer.ipynb
    ├── 03-silverlayer.ipynb
    └── 04-incrementalload.ipynb
README.md
```