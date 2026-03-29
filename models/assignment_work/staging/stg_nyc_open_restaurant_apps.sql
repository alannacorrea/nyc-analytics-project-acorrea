-- Clean and standardize open restaurant data
-- One row per request

WITH source AS (
   SELECT * FROM {{ source('raw_2', 'source_nyc_open_restaurant_apps') }}
), -- Easier to refer to the dbt reference to a long name table this way

cleaned AS (
   SELECT
       -- Get all columns from source, except ones we're transforming below
       -- To do cleaning on them or explicitly cast them as types just in case
       * EXCEPT (
        globalid,
        time_of_submission,
        sidewalk_dimensions_length,
        sidewalk_dimensions_width,
        sidewalk_dimensions_area,
        roadway_dimensions_length,
        roadway_dimensions_width,
        roadway_dimensions_area,
        zip,
        building_number,
        latitude,
        longitude
       ),

       -- Identifiers, removed {} characters to make id searching easier
       REPLACE(REPLACE(CAST(globalid AS STRING), "{", ""), "}", "") AS request_id,

       -- Date/Time
       CAST(time_of_submission AS TIMESTAMP) AS time_of_submission,

       -- Request details
       CASE
        WHEN  UPPER(TRIM(CAST(building_number AS STRING))) = 'UNDEFINED' THEN NULL
        ELSE CAST(building_number AS STRING),
        END AS building_number
       CASE
        WHEN  UPPER(TRIM(CAST(food_service_establishment AS STRING))) = 'PENDING' THEN 'PENDING'
        ELSE CAST(food_service_establishment AS STRING),
        END AS food_service_establishment

       -- Location - clean zip code, handling several common zip code data problems
       CASE
           WHEN UPPER(TRIM(CAST(zip AS STRING))) IN ('N/A', 'NA') THEN NULL
           WHEN UPPER(TRIM(CAST(zip AS STRING))) = 'ANONYMOUS' THEN 'Anonymous'
           WHEN LENGTH(CAST(zip AS STRING)) = 5 THEN CAST(zip AS STRING)
           WHEN LENGTH(CAST(zip AS STRING)) = 9 THEN CAST(zip AS STRING)
           WHEN LENGTH(CAST(zip AS STRING)) = 10
               AND REGEXP_CONTAINS(CAST(zip AS STRING), r'^\d{5}-\d{4}')
           THEN CAST(zip AS STRING)
           ELSE NULL
       END AS zip,

       CAST (sidewalk_dimensions_length AS DECIMAL) AS sidewalk_dimensions_length,
       CAST (sidewalk_dimensions_width AS DECIMAL) AS sidewalk_dimensions_width,
       CAST (sidewalk_dimensions_area AS DECIMAL) AS sidewalk_dimensions_area,
       CAST (roadway_dimensions_length AS DECIMAL) AS roadway_dimensions_length,
       CAST (roadway_dimensions_width AS DECIMAL) AS roadway_dimensions_width,
       CAST (roadway_dimensions_area AS DECIMAL) AS roadway_dimensions_area,
       CAST(latitude AS DECIMAL) AS latitude,
       CAST(longitude AS DECIMAL) AS longitude,

       -- Metadata
       CURRENT_TIMESTAMP() AS _stg_loaded_at

   FROM source

   -- Deduplicate
   QUALIFY ROW_NUMBER() OVER (PARTITION BY globalid ORDER BY time_of_submission DESC) = 1
)

SELECT * FROM cleaned
-- All should be part of this table: stg_nyc_open_restaurant_apps
