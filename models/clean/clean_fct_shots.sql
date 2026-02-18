{{ config(
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='SHOT_ID'
) }}

with src as (
  select *
  from {{ ref('stg_shots_ronaldo_raw') }}
  {% if is_incremental() %}
    where ingestion_batch_id = '{{ var("ingestion_batch_id") }}'
  {% endif %}
)

keyed as (
    select *
    from src
    where match_id is not null
      and shot_id_number is not null
),

prepared as (
    select
        match_id::number(38,0) as match_id,
        shot_id_number::number(38,0) as shot_id_number,

        match_event_id::number(38,0) as match_event_id,
        team_id::number(38,0) as team_id,

        case
            when team_id = 1610612747 then 'Manchester United'
            else coalesce(nullif(trim(team_name), ''), 'UNKNOWN')
        end as team_name,

        case
            when upper(trim(home_away)) in ('HOME', 'H') then 'HOME'
            when upper(trim(home_away)) in ('AWAY', 'A') then 'AWAY'
            else 'UNKNOWN'
        end as home_away,

        try_to_date(date_of_game) as date_of_game,

        location_x::float as location_x,
        location_y::float as location_y,

        remaining_min::float as remaining_min_raw,
        remaining_min_1::float as remaining_min_alt,
        iff(remaining_min is not null and remaining_min_1 is not null and remaining_min <> remaining_min_1, true, false) as remaining_min_conflict_flag,

        remaining_sec::float as remaining_sec_raw,
        remaining_sec_1::float as remaining_sec_alt,
        iff(remaining_sec is not null and remaining_sec_1 is not null and remaining_sec <> remaining_sec_1, true, false) as remaining_sec_conflict_flag,

        coalesce(remaining_min, remaining_min_1)::float as remaining_min,
        coalesce(remaining_sec, remaining_sec_1)::float as remaining_sec,

        (coalesce(remaining_min, remaining_min_1) * 60 + coalesce(remaining_sec, remaining_sec_1))::float as remaining_seconds_total,

        distance_of_shot::float as distance_raw,
        distance_of_shot_1::float as distance_alt,
        iff(distance_of_shot is not null and distance_of_shot_1 is not null and distance_of_shot <> distance_of_shot_1, true, false) as distance_conflict_flag,
        coalesce(distance_of_shot, distance_of_shot_1)::float as distance_of_shot,

        power_of_shot::float as power_raw,
        power_of_shot_1::float as power_alt,
        iff(power_of_shot is not null and power_of_shot_1 is not null and power_of_shot <> power_of_shot_1, true, false) as power_conflict_flag,
        coalesce(power_of_shot, power_of_shot_1)::float as power_of_shot,

        knockout_match::float as knockout_raw,
        knockout_match_1::float as knockout_alt,
        iff(knockout_match is not null and knockout_match_1 is not null and knockout_match <> knockout_match_1, true, false) as knockout_conflict_flag,
        iff(coalesce(knockout_match, knockout_match_1) = 1, true, false) as knockout_match,

        iff(is_goal = 1, true, false) as is_goal,

        nullif(trim(game_season), '') as game_season,
        coalesce(nullif(trim(type_of_shot), ''), 'UNKNOWN') as type_of_shot,
        coalesce(nullif(trim(type_of_combined_shot), ''), 'UNKNOWN') as type_of_combined_shot,
        coalesce(nullif(trim(area_of_shot), ''), 'UNKNOWN') as area_of_shot,
        coalesce(nullif(trim(shot_basics), ''), 'UNKNOWN') as shot_basics,
        coalesce(nullif(trim(range_of_shot), ''), 'UNKNOWN') as range_of_shot,
        nullif(trim(lat_lng), '') as lat_lng,

        ingestion_batch_id,
        source_file_name,
        ingested_at_utc

    from keyed
),

deduped as (
    select *
    from prepared
    qualify row_number() over (
        partition by match_id, shot_id_number
        order by ingested_at_utc desc
    ) = 1
)

select *
from deduped
