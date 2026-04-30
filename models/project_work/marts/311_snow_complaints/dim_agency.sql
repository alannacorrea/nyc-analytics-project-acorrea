--  dimension for NYC service agency lookup 
WITH agency_311 AS (
   SELECT DISTINCT
    agency, 
    agency_name,
      FROM {{ ref('stg_nyc_311') }}
    GROUP BY agency, agency_name
),

agency_dimension AS (
   SELECT
       {{ dbt_utils.generate_surrogate_key([
           'agency',
           'agency_name' 
       ]) }} AS agency_key,
      agency,
      agency_name
     FROM agency_311
)

SELECT * FROM agency_dimension