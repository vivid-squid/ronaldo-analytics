{{ config(materialized='table') }}

select distinct
  team_id,
  team_name
from {{ ref('clean_fct_shots') }}
