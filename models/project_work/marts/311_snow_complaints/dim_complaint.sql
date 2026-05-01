--  dimension for NYC complaint type with details  
WITH complaint_details AS (
   SELECT DISTINCT
   agency,
    complaint_type, 
    descriptor,
    additional_details
      FROM {{ ref('stg_nyc_311') }}
    GROUP BY 1,2,3,4
),

complaint_dimension AS (
   SELECT
       {{ dbt_utils.generate_surrogate_key([
            'agency',
           'complaint_type',
           'descriptor',
           'additional_details'
       ]) }} AS complaint_key,
    *
     FROM complaint_details
)

SELECT * FROM complaint_dimension