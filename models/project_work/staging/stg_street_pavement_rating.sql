-- Clean and standardize 311 DOT service request data
-- One row per service request

WITH source AS (
   SELECT * FROM {{ source('raw', 'street_pavement_data') }}
),

cleaned AS (
   SELECT

       * EXCEPT (
           oftcode,
           inspection, 
           boroughname,
           ismultipass, 
           systemrating,
           locationgeometry_stlength
       ),

       -- Identifiers
       CAST(oftcode AS STRING) AS oftcode,

       -- Date/Time
       CAST(inspection AS TIMESTAMP) AS inspection_date,

       -- Cast Values
       CAST(ismultipass AS STRING) AS ismultipass,
       CAST(systemrating AS DECIMAL) AS systemrating,
       CAST(locationgeometry_stlength AS FLOAT) AS locationgeometry_stlength,

       -- Location - standardized borough, just in case
       CASE
           WHEN boroughname IS NULL THEN  'UNKNOWN or CITYWIDE'
           ELSE boroughname
       END AS borough,

       -- Metadata
       CURRENT_TIMESTAMP() AS _stg_loaded_at

   FROM source

   -- Filters
   WHERE 
   oftcode IS NOT NULL
   AND inspection IS NOT NULL
   AND CAST(inspection AS DATE) >= DATE_SUB(CURRENT_DATE(), INTERVAL 10 YEAR)
   AND boroughname IS NOT NULL

   -- Deduplicate
   QUALIFY ROW_NUMBER() OVER (PARTITION BY oftcode ORDER BY inspection_date DESC) = 1
)

SELECT * FROM cleaned
-- All should be part of this table: stg_street_pavement_rating
