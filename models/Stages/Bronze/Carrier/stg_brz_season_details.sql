{{
    config(
        materialized='table',
        tags = ["bronze","season"]
    )
}}

with season as(
    SELECT
        {{ dbt_utils.star(from=ref('stg_raw_season_details')) }}
    FROM {{ ref('stg_raw_season_details') }}
)

SELECT * FROM season