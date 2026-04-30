--  dimension for NYC service agency lookup 
WITH 311_agency AS (
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
     FROM 311_agency
)

SELECT * FROM agency_dimension