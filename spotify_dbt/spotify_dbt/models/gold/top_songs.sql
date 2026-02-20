{{ 
    config(
        schema='mart'
    ) 
}}

SELECT 
    SONG_ID,
    SONG_NAME,
    ARTIST_NAME,
    COUNT(CASE WHEN EVENT_TYPE = 'play' THEN 1 ELSE 0 END) AS TOTAL_PLAYS,
    COUNT(CASE WHEN EVENT_TYPE = 'skip' THEN 1 ELSE 0 END) AS  TOTAL_SKIPS
FROM 
{{
    ref('spotify_silver')    
}}
GROUP BY SONG_ID, SONG_NAME, ARTIST_NAME
ORDER BY TOTAL_PLAYS DESC