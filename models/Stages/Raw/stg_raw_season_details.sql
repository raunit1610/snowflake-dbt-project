{{
    config(
        materialized='table',
        tags = ["raw","season"]
    )
}}

with season as(
    SELECT 
        {{ dbt_utils.star(from=ref('stg_raw_crr_seasons_details'), except=["FILENAME", "FEEDSOURCE", "STATSFEED", "TIMEZONE", "SOURCE_LOAD_TIMESTAMP", "_INSERTED_AT_"]) }}
    FROM {{ ref('stg_raw_crr_seasons_details') }}
)

SELECT * FROM season