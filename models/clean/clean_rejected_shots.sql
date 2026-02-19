{{ config(materialized='table') }}

select
  {{ dbt_utils.generate_surrogate_key([
      'ingestion_batch_id',
      'match_id',
      'shot_id_number',
      'match_event_id'
  ]) }} as reject_id,
  *,
  'NULL_SHOT_ID_NUMBER' as reject_reason
from {{ ref('stg_shots_ronaldo_raw') }}
where shot_id_number is null
