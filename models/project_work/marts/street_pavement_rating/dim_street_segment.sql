--  dimension for street location
WITH street_segement AS (
   SELECT DISTINCT
      oftcode, 
      onstreetna, 
      fromstreet, 
      tostreetna, 
      direction, 
      road_type, 
      ismultipass,
   FROM {{ ref('stg_street_pavement_rating') }}
   WHERE oftcode IS NOT NULL
),

street_dimension AS (
   SELECT DISTINCT
       {{ dbt_utils.generate_surrogate_key([
           'oftcode'
       ]) }} AS street_dimension_key,
            oftcode, 
      onstreetna, 
      fromstreet, 
      tostreetna, 
      direction, 
      road_type, 
      ismultipass,
     FROM street_segement
)

SELECT * FROM street_dimension