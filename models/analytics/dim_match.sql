{{ config(materialized='table') }}

with base as (
    select
        match_id,
        date_of_game,
        game_season,
        knockout_match
    from {{ ref('clean_fct_shots') }}
)

select
    match_id,

    -- choose a stable canonical date per match
    min(date_of_game) as date_of_game,

    -- pick a stable season representation (simple deterministic approach)
    max(game_season) as game_season,

    -- if any row flags knockout, treat the match as knockout
    max(iff(knockout_match, 1, 0)) = 1 as knockout_match

from base
group by match_id
