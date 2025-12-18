{{
    config(
        materialized='table',
        tags = ["raw","batting","bowling"]
    )
}}

with playerstats as(
    SELECT
        COALESCE(a.PLAYER_ID,b.PLAYER_ID) AS PLAYER_ID
        , COALESCE(a.PLAYER_NAME,b.PLAYER_NAME) AS PLAYER_NAME
        , COALESCE(a.YEAR_TYPE,b.YEAR_TYPE) AS YEAR_TYPE
        , COALESCE(a.TEAM_NAME,b.TEAM_NAME) AS TEAM_NAME
        , GREATEST(COALESCE(a.MATCHES,0),COALESCE(b.MATCHES,0)) AS MATCHES
        , GREATEST(COALESCE(a.INNINGS,0),COALESCE(b.INNINGS,0)) AS INNINGS
        , (CASE
            WHEN a.STUMPINGS IS NOT NULL AND a.STUMPINGS <> 0 THEN 'Wicket Keeper'
            WHEN b.OVERS_DELIVERED = 0 AND a.STUMPINGS = 0 THEN 'Batsman'
            WHEN a.BALLS_TAKEN IS NULL THEN 'Bowler'
            ELSE 'Bowler or All-Rounder' 
          END)::VARCHAR AS PLAYER_TYPE
        , {{ dbt_utils.star(from=ref('stg_raw_crr_batting_fielding_details'), except=["PLAYER_ID", "PLAYER_NAME", "YEAR_TYPE", "TEAM_NAME", "MATCHES", "INNINGS", "STATS_TYPE", "FILE_NAME", "SOURCE_LOAD_TIMESTAMP", "_INSERTED_AT_"
])}}
        , {{dbt_utils.star(from=ref('stg_raw_crr_bowling_details'), except=["PLAYER_ID", "PLAYER_NAME", "YEAR_TYPE", "TEAM_NAME", "MATCHES", "INNINGS", "STATS_TYPE", "FILE_NAME", "SOURCE_LOAD_TIMESTAMP", "_INSERTED_AT_"])}}
    FROM {{ ref('stg_raw_crr_batting_fielding_details') }} a
    FULL OUTER JOIN {{ ref('stg_raw_crr_bowling_details') }} b ON a.PLAYER_ID = b.PLAYER_ID
    ORDER BY PLAYER_ID
)

SELECT * FROM playerstats