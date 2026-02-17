{{ config(materialized='table') }}

select
    *,
    'NULL_SHOT_ID_NUMBER' as reject_reason
from {{ ref('stg_shots_ronaldo_raw') }}
where shot_id_number is null
