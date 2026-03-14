-- 0) warehouse visibility
SHOW WAREHOUSES LIKE 'COMPUTE_WH';

-- 1) Database + schemas
CREATE DATABASE IF NOT EXISTS RONALDO_DB;
USE DATABASE RONALDO_DB;

CREATE SCHEMA IF NOT EXISTS RAW;
CREATE SCHEMA IF NOT EXISTS STAGING;
CREATE SCHEMA IF NOT EXISTS CLEAN;
CREATE SCHEMA IF NOT EXISTS ANALYTICS;
CREATE SCHEMA IF NOT EXISTS META;

-- 2) RAW landing table (ADF target)

USE SCHEMA RAW;

CREATE TABLE IF NOT EXISTS RAW.SHOTS_RONALDO_RAW (
  UNNAMED_0               NUMBER,
  MATCH_EVENT_ID          FLOAT,
  LOCATION_X              FLOAT,
  LOCATION_Y              FLOAT,
  REMAINING_MIN           FLOAT,
  POWER_OF_SHOT           FLOAT,
  KNOCKOUT_MATCH          FLOAT,
  GAME_SEASON             STRING,
  REMAINING_SEC           FLOAT,
  DISTANCE_OF_SHOT        FLOAT,
  IS_GOAL                 FLOAT,
  AREA_OF_SHOT            STRING,
  SHOT_BASICS             STRING,
  RANGE_OF_SHOT           STRING,
  TEAM_NAME               STRING,
  DATE_OF_GAME            STRING,
  HOME_AWAY               STRING,
  SHOT_ID_NUMBER          FLOAT,
  LAT_LNG                 STRING,
  TYPE_OF_SHOT            STRING,
  TYPE_OF_COMBINED_SHOT   STRING,
  MATCH_ID                NUMBER,
  TEAM_ID                 NUMBER,

  -- duplicate columns from source extracts
  REMAINING_MIN_1         FLOAT,
  POWER_OF_SHOT_1         FLOAT,
  KNOCKOUT_MATCH_1        FLOAT,
  REMAINING_SEC_1         FLOAT,
  DISTANCE_OF_SHOT_1      FLOAT,

  -- ingestion metadata (populated by ADF)
  INGESTION_BATCH_ID      STRING,
  SOURCE_FILE_NAME        STRING,
  INGESTED_AT_UTC         TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- 3) META.LOAD_AUDIT (observability)
USE SCHEMA META;

CREATE TABLE IF NOT EXISTS META.LOAD_AUDIT (
  INGESTION_BATCH_ID   STRING,
  PIPELINE_NAME        STRING,
  SOURCE_FILE_NAME     STRING,
  TARGET_TABLE         STRING,
  ROWS_LOADED          NUMBER,
  LOAD_STATUS          STRING,         -- STARTED / SUCCESS / FAILED / TRANSFORM_STARTED / TRANSFORM_SUCCESS / TRANSFORM_FAILED
  STARTED_AT_UTC       TIMESTAMP_NTZ,
  ENDED_AT_UTC         TIMESTAMP_NTZ,
  ERROR_MESSAGE        STRING
);

-- 4) Sanity checks (read-only)
-- 4.1 Count rows in RAW
SELECT COUNT(*) AS raw_row_count
FROM RONALDO_DB.RAW.SHOTS_RONALDO_RAW;

-- 4.2 List loaded batches
SELECT
  SOURCE_FILE_NAME,
  INGESTION_BATCH_ID,
  COUNT(*)               AS rows_loaded,
  MIN(INGESTED_AT_UTC)   AS first_ingest_utc,
  MAX(INGESTED_AT_UTC)   AS last_ingest_utc
FROM RONALDO_DB.RAW.SHOTS_RONALDO_RAW
GROUP BY 1,2
ORDER BY last_ingest_utc DESC;

-- 4.3 Confirm dbt-built objects exist (after dbt runs)
SHOW VIEWS  IN SCHEMA RONALDO_DB.STAGING;
SHOW TABLES IN SCHEMA RONALDO_DB.CLEAN;
SHOW TABLES IN SCHEMA RONALDO_DB.ANALYTICS;

-- 4.4 Quick analytics validation (after dbt runs)
SELECT COUNT(*) AS clean_row_count
FROM RONALDO_DB.CLEAN.CLEAN_FCT_SHOTS;

SELECT COUNT(*) AS analytics_row_count
FROM RONALDO_DB.ANALYTICS.FACT_SHOTS;

SELECT * FROM RONALDO_DB.ANALYTICS.FACT_SHOTS limit 20;

select home_away, count(*) 
from ANALYTICS.fact_shots
group by 1
order by 2 desc;

select home_away, count(*) from RAW.SHOTS_RONALDO_RAW group by 1 order by 2 desc;


select
  sum(case when type_of_shot is null or trim(type_of_shot)='' then 1 else 0 end) as blank_type_of_shot,
  sum(case when type_of_combined_shot is null or trim(type_of_combined_shot)='' then 1 else 0 end) as blank_type_of_combined_shot,
  sum(case when area_of_shot is null or trim(area_of_shot)='' then 1 else 0 end) as blank_area_of_shot,
  count(*) as total
from RAW.SHOTS_RONALDO_RAW;


--view checks

select count(*) from ANALYTICS.fact_shots;
select count(*) from CLEAN.clean_fct_shots;
select count(*) from CLEAN.clean_rejected_shots;


-- How many raw rows were in this batch?
select count(*)
from RAW.shots_ronaldo_raw
where ingestion_batch_id = '<latest_batch_id>';

-- How many raw rows would be rejected (shot_id_number null)?
select count(*)
from RAW.shots_ronaldo_raw
where ingestion_batch_id = '<latest_batch_id>'
  and shot_id_number is null;

-- How many raw rows would be rejected (match_id null)?
select count(*)
from RAW.shots_ronaldo_raw
where ingestion_batch_id = '<latest_batch_id>'
  and match_id is null;


SELECT COUNT(*) FROM ANALYTICS.FACT_SHOTS;

SELECT ingestion_batch_id, COUNT(*)
FROM ANALYTICS.FACT_SHOTS
GROUP BY ingestion_batch_id;




SELECT
    'RAW' AS layer,
    COUNT(*) AS row_count
FROM RAW.SHOTS_RONALDO_RAW

UNION ALL

SELECT
    'CLEAN_VALID' AS layer,
    COUNT(*) AS row_count
FROM CLEAN.CLEAN_FCT_SHOTS

UNION ALL

SELECT
    'CLEAN_REJECTED' AS layer,
    COUNT(*) AS row_count
FROM CLEAN.CLEAN_REJECTED_SHOTS;

---------------------==================================

select
  count(*) as total_raw,
  sum(case when match_id is null then 1 else 0 end) as null_match_id,
  sum(case when shot_id_number is null then 1 else 0 end) as null_shot_id_number,
  sum(case when match_id is null or shot_id_number is null then 1 else 0 end) as null_any_key
from RAW.SHOTS_RONALDO_RAW;


select
  count(*) as total_raw,
  count(distinct match_id || '-' || shot_id_number) as distinct_match_shot,
  count(*) - count(distinct match_id || '-' || shot_id_number) as duplicate_rows
from RAW.SHOTS_RONALDO_RAW
where match_id is not null
  and shot_id_number is not null;


select
  avg(cnt) as avg_rows_per_match_shot,
  max(cnt) as max_rows_per_match_shot
from (
  select match_id, shot_id_number, count(*) as cnt
  from RAW.SHOTS_RONALDO_RAW
  where match_id is not null
    and shot_id_number is not null
  group by 1,2
);


------------===========================

/*-------------------------------------------------------
--Drop all the views and tables in analytics for PowerBI

USE DATABASE RONALDO_DB;
USE SCHEMA ANALYTICS;

DROP TABLE IF EXISTS RONALDO_DB.ANALYTICS.DIM_MATCH;
DROP TABLE IF EXISTS RONALDO_DB.ANALYTICS.DIM_TEAM;
DROP TABLE IF EXISTS RONALDO_DB.ANALYTICS.DQ_RUN_SUMMARY;
DROP TABLE IF EXISTS RONALDO_DB.ANALYTICS.FACT_SHOTS;

-- Confirm it's empty
SHOW TABLES IN SCHEMA RONALDO_DB.ANALYTICS;
SHOW VIEWS  IN SCHEMA RONALDO_DB.ANALYTICS;

*/