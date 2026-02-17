{{ config(materialized='table') }}

with base as (
    select *
    from {{ ref('clean_fct_shots') }}
),

conflicts as (
    select
        count(*) as clean_rows,

        sum(iff(remaining_min_conflict_flag, 1, 0)) as remaining_min_conflicts,
        sum(iff(remaining_sec_conflict_flag, 1, 0)) as remaining_sec_conflicts,
        sum(iff(distance_conflict_flag, 1, 0)) as distance_conflicts,
        sum(iff(power_conflict_flag, 1, 0)) as power_conflicts,
        sum(iff(knockout_conflict_flag, 1, 0)) as knockout_conflicts

    from base
),

rejected as (
    select count(*) as rejected_rows
    from {{ ref('clean_rejected_shots') }}
)

select
    current_timestamp() as measured_at_utc,
    c.clean_rows,
    r.rejected_rows,

    c.remaining_min_conflicts,
    c.remaining_sec_conflicts,
    c.distance_conflicts,
    c.power_conflicts,
    c.knockout_conflicts,

    -- rates
    (c.remaining_min_conflicts / nullif(c.clean_rows, 0))::float as remaining_min_conflict_rate,
    (c.remaining_sec_conflicts / nullif(c.clean_rows, 0))::float as remaining_sec_conflict_rate,
    (c.distance_conflicts / nullif(c.clean_rows, 0))::float as distance_conflict_rate,
    (c.power_conflicts / nullif(c.clean_rows, 0))::float as power_conflict_rate,
    (c.knockout_conflicts / nullif(c.clean_rows, 0))::float as knockout_conflict_rate

from conflicts c
cross join rejected r
