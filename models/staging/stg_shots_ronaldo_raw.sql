select *
from {{ source('raw', 'shots_ronaldo_raw') }}
