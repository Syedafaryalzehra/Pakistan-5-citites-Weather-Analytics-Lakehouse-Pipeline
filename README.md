# Pakistan 5-Cities Weather Analytics — Lakehouse Pipeline (Phases 1–3)

PySpark pipeline on Databricks implementing the **Bronze**, **Silver** and **Gold** layers of a medallion architecture, served to Power BI.

- **Source:** Open-Meteo Historical Weather API (hourly data)
- **Cities:** Lahore, Karachi, Islamabad, Peshawar, Quetta
- **Platform:** Databricks (Delta Lake), database `pakistan_weather`

## 1. Architecture

```
Open-Meteo API ──► Raw JSON (Volumes) ──► Bronze (raw, all strings) ──► Silver (typed, deduplicated)
                                              │                              │        │
                                              │                     silver_quarantine  │
                                              │                              │        │
                                              └── pipeline_execution_logs ◄──┴────────┤
                                                                                     │
                                                     Gold star schema  ◄─────────────┘
                                                     dim_location · dim_date
                                                     fact_weather_hourly (clustered)
                                                     agg_monthly_climate · agg_yearly_extremes
                                                                                     │
                                                                          Power BI (Phase3/*.pbix)
```

| Layer | Table | Behaviour |
|---|---|---|
| Bronze | `bronze_weather_raw` | Raw landing zone. Explicit schema, every value kept as a string. Full load = overwrite (one time), incremental = append. |
| Silver | `silver_weather_cleaned` | One row per city per hour. Types cast, derived columns added, loaded with `MERGE INTO`. |
| Quarantine | `silver_quarantine` | Rows that could not be cast to the expected types. |
| Gold — Dim | `dim_location`, `dim_date` | Conformed dimensions: 5 cities; calendar 2000 → latest loaded year. Rebuilt with `overwriteSchema`. |
| Gold — Fact | `fact_weather_hourly` | One row per city per hour. `MERGE INTO` + liquid clustering on `(location_key, date_key)`. |
| Gold — Agg | `agg_monthly_climate`, `agg_yearly_extremes` | Monthly and yearly rollups rebuilt from the fact each run. |
| Audit | `pipeline_execution_logs` | One row per layer run — Bronze, Silver and Gold. |
| Serving | Power BI | Report on the Gold tables; file in `Phase3/`. |

## 2. Notebooks

| Notebook | Purpose |
|---|---|
| `01-setup` | Creates the `pakistan_weather` database and the `pipeline_execution_logs` table. |
| `02-bronzelayer` | Raw JSON → Bronze. Reads with an explicit `StructType` (no `inferSchema`). |
| `03-silverlayer` | Bronze → Silver. Explodes hourly arrays, casts types, quarantines bad rows, `MERGE INTO`. |
| `04-incrementalload` | Fetches one day or a date range from the API, saves the raw file, then runs `02`, `03` and `05`. |
| `05-goldlayer` | Silver → Gold. Builds `dim_location`, `dim_date`, `fact_weather_hourly` (`MERGE`, clustered), both aggregates, validation checks and the audit row. |

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
| `layer` | STRING | `Raw-to-Bronze`, `Bronze-to-Silver` or `Silver-to-Gold` |
| `load_type` | STRING | `full` or `incremental` |
| `file_processed` | STRING | Raw file path (Bronze) or source table (Silver) |
| `city` | STRING | `All-5-Cities` |
| `start_time` | TIMESTAMP | Run start |
| `end_time` | TIMESTAMP | Run end |
| `status` | STRING | `Success` or `Failure` |
| `rows_inserted` | BIGINT | Rows inserted (Bronze: rows written; Silver: `MERGE` inserts) |
| `rows_updated` | BIGINT | Rows updated (Silver/Gold `MERGE` updates) |
| `rows_aggregated` | BIGINT | Rows read from Silver by the Gold run |
| `error_message` | STRING | Error text on failure, otherwise NULL |
| `batch_id` | STRING | Batch identifier |

### 3.5 Gold Dimensions — `dim_location`, `dim_date`

**`dim_location`** (5 rows). **Primary key:** `location_key`.

| Column | Type | Description |
|---|---|---|
| `location_key` | BIGINT | Surrogate key, 1–5 |
| `city` | STRING | Lahore, Karachi, Islamabad, Peshawar, Quetta |
| `province` | STRING | Province / territory |
| `grid_latitude`, `grid_longitude` | DOUBLE | API grid point (joins back to Silver lat/long) |
| `city_latitude`, `city_longitude` | DOUBLE | City centre coordinates |

**`dim_date`** (one row per day, 2000-01-01 → latest year in Silver). **Primary key:** `date_key`.

| Column | Type | Description |
|---|---|---|
| `date_key` | INT | Smart key `YYYYMMDD` |
| `full_date` | DATE | Calendar date |
| `year`, `quarter`, `month`, `day`, `day_of_week` | INT | Calendar hierarchy |
| `month_name`, `day_name` | STRING | Human-readable names |
| `is_weekend` | BOOLEAN | Saturday/Sunday |
| `season` | STRING | Spring / Summer / Autumn / Winter |
| `is_monsoon` | BOOLEAN | July–September |

### 3.6 Gold Fact — `fact_weather_hourly`

**Grain:** one row per city per hour. **Primary key:** `(location_key, observation_timestamp)` — this is the `MERGE INTO` key. **Clustering:** `CLUSTER BY (location_key, date_key)`.

| Column | Type | Description |
|---|---|---|
| `location_key` | BIGINT | PK, FK → `dim_location` |
| `observation_timestamp` | TIMESTAMP | PK; hour of observation |
| `date_key` | INT | FK → `dim_date` |
| `hour` | INT | 0–23 |
| `time_of_day` | STRING | Derived: Morning / Afternoon / Evening / Night |
| `heat_index` | DOUBLE | Derived: heat-index approximation for temps ≥ 27 °C, else equals `temperature_2m` |
| `gold_load_timestamp` | TIMESTAMP | When the row was written to Gold |
| `batch_id` | STRING | Batch identifier |

**Measures:** `temperature_2m`, `apparent_temperature`, `relative_humidity_2m`, `precipitation`, `rain`, `wind_speed_10m`, `wind_gusts_10m`, `pressure_msl`, `cloud_cover`, `shortwave_radiation`, `et0_fao_evapotranspiration`.

### 3.7 Gold Aggregates — `agg_monthly_climate`, `agg_yearly_extremes`

**`agg_monthly_climate`** — logical key `(location_key, year, month)`; `date_key` points to the month-start date (`YYYYMM01`) in `dim_date` (shared dimension at coarser grain).

| Column | Type | Description |
|---|---|---|
| `location_key` | BIGINT | FK → `dim_location` |
| `year`, `month`, `month_name` | INT, STRING | Grain |
| `date_key` | INT | FK → month-start `dim_date.date_key` |
| `avg_temp_c`, `min_temp_c`, `max_temp_c` | DOUBLE | Monthly temperature stats (°C) |
| `total_precip_mm` | DOUBLE | Monthly total precipitation (mm) |
| `avg_wind_speed_kmh` | DOUBLE | Monthly mean wind speed |
| `hours_observed` | BIGINT | Hourly rows behind the aggregate (~744) |
| `gold_load_timestamp`, `batch_id` | TIMESTAMP, STRING | Audit / lineage |

**`agg_yearly_extremes`** — logical key `(location_key, year)`; `date_key` points to the year-start date (`YYYY0101`).

| Column | Type | Description |
|---|---|---|
| `location_key` | BIGINT | FK → `dim_location` |
| `year` | INT | Grain |
| `date_key` | INT | FK → year-start `dim_date.date_key` |
| `hottest_temp_c` | DOUBLE | Hottest daily max (°C) |
| `coldest_temp_c` | DOUBLE | Coldest daily min (°C) |
| `wettest_day_precip_mm` | DOUBLE | Highest single-day rainfall (mm) |
| `highest_wind_speed_kmh` | DOUBLE | Strongest daily peak wind |
| `gold_load_timestamp`, `batch_id` | TIMESTAMP, STRING | Audit / lineage |

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

**Audit logging.** Every run of `02` and `03`, full or incremental, writes a row to `pipeline_execution_logs` with layer, file/table processed, start and end time, status and row counts. Failures are logged first and then re-raised, so a failed Bronze run stops `04` before Silver runs. The Gold run (`05`) writes the `Silver-to-Gold` row with `rows_aggregated`.

**Star schema & broadcast joins.** The fact joins Silver's grid coordinates to the nearest city (squared-distance window in `dim_location`), then both dimensions are broadcast-joined — they are tiny (5 and ~9.8 K rows), so the 1.17 M-row fact is never shuffled.

**Liquid clustering.** `fact_weather_hourly` uses `CLUSTER BY (location_key, date_key)` because every analytical query filters by city and date. Unlike Hive-style partitioning it adapts as data grows and avoids small-file and over-partitioning problems.

**Gold idempotency & cost control.** The fact is loaded with `MERGE INTO` on `(location_key, observation_timestamp)` — re-running a batch updates in place, never duplicates. Dimensions and aggregates are rebuilt with `overwrite`, safe because they are fully derived from the fact. `OPTIMIZE` runs only on full loads so daily incremental runs skip compaction (FinOps).

**Derived measures.** `time_of_day` buckets and the `heat_index` formula (applied at ≥ 27 °C) are computed once in Gold, so every consumer gets identical definitions.

**Gold validation.** After every Gold run: row counts per table, exactly 5 locations, zero duplicate fact keys and zero orphan `location_key` / `date_key` values — enforced with assertions that fail the run on violation.

## 5. Execution Guide

Run `00-dataaquisition_fullload and 01-setup` once before anything else.

### 5.1 Initial full load (one time)

1. Run `02-bronzelayer` with widgets:
   - `load_type` = `full`
   - `batch_id` = `batch_fullload_2000_2026`
   - `raw_file` = path to the full-load JSON file in Volumes
2. Run `03-silverlayer` with widgets:
   - `load_type` = `full`
   - `batch_id` = `batch_fullload_2000_2026`

### 5.2 Standard incremental load (daily)

Run `04-incrementalload` with no changes. date defaults to yesterday. The notebook:
1. Fetches that date for all 5 cities and saves `incremental_<date>.json`
2. Runs `02-bronzelayer` (append, `batch_id = batch_incremental_<date>`)
3. Runs `03-silverlayer` (`MERGE` into Silver)

### 5.3 Backfill

| Goal | What to run | Parameters |
|---|---|---|
| Re-load one historical day end to end | `04-incrementalload` | start_date = end_date = YYYY-MM-DD for one day |
| Re-load a range of days | `04-incrementalload` once per date | start_date = first day, end_date = last day (inclusive) |
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

### 5.5 Gold after a full load

Run `05-goldlayer` with widgets `load_type` = `full`, `batch_id` = `batch_fullload_2000_2026`. Rebuilds the dimensions and aggregates and merges the full history into the fact.

### 5.6 Gold on the daily incremental

No extra step: `04-incrementalload` sets `LOAD_TYPE = "incremental"` and runs `05-goldlayer` automatically after Silver — only that day's batch is merged into the fact.

## 6. Repository Structure

```
Phase3/
├── Final Architecture & FinOps Report pakistan weather.pdf
└── Pakistan_5cities_weather_pipeline_visualreport.pbix

data/
├── fetch_data.py
├── Full_load/full_load_sample.json
└── incremental_load/inc_load.json

phase-1/ (proposal documents)

phase2/
└── notebooks/
    ├── 00-dataacqusition_fullload.ipynb
    ├── 01-setup.ipynb
    ├── 02-bronzelayer.ipynb
    ├── 03-silverlayer.ipynb
    ├── 04-incrementalload.ipynb
    └── 05-goldlayer.ipynb

README.md
```
%md
# Phase 3 — Gold Layer

Builds the **Gold** star schema on top of Silver: two shared dimensions, one clustered hourly fact, and two aggregate rollups, plus automated data-quality validation after every load.

## 1. Architecture (updated)

```
Open-Meteo API ──► Raw JSON (Volumes) ──► Bronze ──► Silver ──► Gold (star schema)
                                                        │            │
                                                        │     ┌──────┴──────┐
                                                        │   dim_location dim_date
                                                        │        │       │
                                                        └── fact_weather_hourly (CLUSTER BY location_key, date_key)
                                                                  │
                                                   agg_monthly_climate · agg_yearly_extremes
                                                        │
                                              pipeline_execution_logs (audit)
```

| Layer | Table | Behaviour |
|---|---|---|
| Dim | `dim_location` | 5 rows (one per city). Rebuilt with `overwriteSchema` every Gold run. |
| Dim | `dim_date` | One row per day, 2000-01-01 → max year in Silver, with calendar hierarchy + `season`, `is_weekend`, `is_monsoon`. |
| Fact | `fact_weather_hourly` | One row per city per hour. Loaded with `MERGE INTO`, **liquid clustered** by `(location_key, date_key)`. |
| Agg | `agg_monthly_climate` | Monthly rollup per city (avg/min/max temp, total precip, avg wind, hours observed). Rebuilt full-table each run. |
| Agg | `agg_yearly_extremes` | Yearly extremes per city (hottest, coldest, wettest day, highest wind). Rebuilt full-table each run. |

## 2. Notebooks (added)

| Notebook | Purpose |
|---|---|
| `05-goldlayer` | Silver → Gold. Builds `dim_location` (nearest-city grid mapping), `dim_date` (dynamic through max Silver year), `fact_weather_hourly` (MERGE + derived `time_of_day` / `heat_index`), both aggregates, `OPTIMIZE` (full loads only), validation checks, and the `Silver-to-Gold` audit row. |

## 3. Data Dictionary (Gold)

### 3.5 `dim_location` — 5 rows
**Primary key:** `location_key`.

| Column | Type | Description |
|---|---|---|
| `location_key` | BIGINT | Surrogate key, 1–5 |
| `city` | STRING | Lahore, Karachi, Islamabad, Peshawar, Quetta |
| `province` | STRING | Province / territory |
| `grid_latitude`, `grid_longitude` | DOUBLE | API grid point (joins back to Silver lat/long) |
| `city_latitude`, `city_longitude` | DOUBLE | City centre coordinates |

### 3.6 `dim_date` — one row per day, 2000 → latest loaded year
**Primary key:** `date_key` (smart key `YYYYMMDD`).

| Column | Type | Description |
|---|---|---|
| `date_key` | INT | `YYYYMMDD` |
| `full_date` | DATE | Calendar date |
| `year`, `quarter`, `month`, `day`, `day_of_week` | INT | Calendar hierarchy |
| `month_name`, `day_name` | STRING | Human-readable |
| `is_weekend` | BOOLEAN | Sat/Sun |
| `season` | STRING | Spring / Summer / Autumn / Winter |
| `is_monsoon` | BOOLEAN | Jul–Sep |

### 3.7 `fact_weather_hourly` — one row per city per hour
**Grain:** city + hour. **Primary key:** `(location_key, observation_timestamp)` — this is the `MERGE` key.

| Column | Type | Description |
|---|---|---|
| `location_key` | BIGINT | FK → `dim_location` |
| `date_key` | INT | FK → `dim_date` |
| `observation_timestamp` | TIMESTAMP | Hour of observation |
| `hour` | INT | 0–23 |
| `time_of_day` | STRING | Derived: Morning / Afternoon / Evening / Night |
| `heat_index` | DOUBLE | Derived: NOAA-style approximation, only for temps ≥ 27 °C, else equals `temperature_2m` |
| `gold_load_timestamp` | TIMESTAMP | Load time |
| `batch_id` | STRING | Batch identifier |

**Measures:** `temperature_2m`, `apparent_temperature`, `relative_humidity_2m`, `precipitation`, `rain`, `wind_speed_10m`, `wind_gusts_10m`, `pressure_msl`, `cloud_cover`, `shortwave_radiation`, `et0_fao_evapotranspiration`.

### 3.8 `agg_monthly_climate` / `agg_yearly_extremes`

**Monthly** — logical key `(location_key, year, month)`; `date_key` points to the month-start date (`YYYYMM01`) in `dim_date` (shared dimension at coarser grain). Columns: `avg_temp_c`, `min_temp_c`, `max_temp_c`, `total_precip_mm`, `avg_wind_speed_kmh`, `hours_observed` (~744/city/month), `gold_load_timestamp`, `batch_id`.

**Yearly** — logical key `(location_key, year)`; `date_key` points to the year-start date (`YYYY0101`). Columns: `hottest_temp_c`, `coldest_temp_c`, `wettest_day_precip_mm`, `highest_wind_speed_kmh`, `gold_load_timestamp`, `batch_id`.

### 3.4 Audit table update
`pipeline_execution_logs` now also receives `Silver-to-Gold` runs (`rows_inserted` / `rows_updated` from the fact MERGE metrics, `rows_aggregated` = fact rows processed).

## 4. Engineering Design (added)

**Star schema.** The fact joins Silver's grid coordinates to the nearest city (squared-distance window), then both dimensions are broadcast-joined — dimensions are tiny (5 and ~9.8 K rows), so broadcast avoids shuffling the 1.17 M-row fact.

**Liquid clustering, not partitioning.** `fact_weather_hourly` uses `CLUSTER BY (location_key, date_key)` because every analytical query filters by city and date. Unlike Hive-style partitioning, it adapts as data grows and avoids small-file and over-partitioning problems.

**Idempotency.** The fact is loaded with `MERGE INTO` on `(location_key, observation_timestamp)` — re-running any batch updates in place, never duplicates. Dimensions and aggregates are rebuilt with `overwrite`, which is safe because they are fully derived from the fact.

**Cost control (FinOps).** `OPTIMIZE` runs only on full loads; daily incremental runs skip compaction and stay cheap.

**Derived measures.** `time_of_day` buckets and the `heat_index` formula (applied at ≥ 27 °C) are computed once in Gold, so every consumer gets identical definitions.

**Automated validation.** After every Gold run: row counts per table, exactly 5 locations expected, zero duplicate fact keys, zero orphan `location_key` / `date_key` values (referential integrity), enforced with assertions that fail the run on violation.

## 5. Execution Guide (added)

### 5.5 Gold after a full Silver load
Run `05-goldlayer` with `load_type` = `full`, `batch_id` = `batch_fullload_2000_2026`.

### 5.6 Gold on the daily incremental
No extra step: `04-incrementalload` now sets `LOAD_TYPE = "incremental"` and runs Gold automatically after Silver, MERGE-ing only that day's batch into the fact and rebuilding aggregates.

## 6. Repository Structure

```
Phase3/
├── Final Architecture & FinOps Report pakistan weather.pdf
└── Pakistan_5cities_weather_pipeline_visualreport.pbix

data/
├── fetch_data.py
├── Full_load/full_load_sample.json
└── incremental_load/inc_load.json

phase-1/ (proposal documents)

phase2/
└── notebooks/
    ├── 00-dataacqusition_fullload.ipynb
    ├── 01-setup.ipynb
    ├── 02-bronzelayer.ipynb
    ├── 03-silverlayer.ipynb
    ├── 04-incrementalload.ipynb
    └── 05-goldlayer.ipynb

README.md
```



