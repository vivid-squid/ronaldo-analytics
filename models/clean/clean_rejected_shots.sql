{{ config(materialized='table') }}

with src as (
  select *
  from {{ ref('stg_shots_ronaldo_raw') }}
  {% if var("ingestion_batch_id", "") != "" %}
    where ingestion_batch_id = '{{ var("ingestion_batch_id") }}'
  {% endif %}
)

select
  {{ dbt_utils.generate_surrogate_key([
      'ingestion_batch_id',
      'source_file_name',
      'match_event_id',
      'unnamed_0'
  ]) }} as reject_id,
  *,
  'NULL_SHOT_ID_NUMBER' as reject_reason
from src
where shot_id_number is null
