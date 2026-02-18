{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='SHOT_ID'
) }}

with src as (
    select *
    from {{ ref('clean_fct_shots') }}
    {% if is_incremental() %}
      -- only process new batch
      where ingestion_batch_id = '{{ var("ingestion_batch_id") }}'
    {% endif %}
)

select
  -- keys
  match_id,
  shot_id_number,
  team_id,

  -- match/team descriptors that are okay in fact too
  home_away,
  date_of_game,
  game_season,
  knockout_match,

  -- measures
  is_goal,
  distance_of_shot,
  power_of_shot,
  remaining_seconds_total,

  -- shot descriptors (useful slicers)
  type_of_shot,
  type_of_combined_shot,
  area_of_shot,
  shot_basics,
  range_of_shot,

  -- lineage (optional to expose)
  ingestion_batch_id,
  source_file_name,
  ingested_at_utc

from {{ ref('clean_fct_shots') }}
