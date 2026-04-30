--  dimension for NYC complaint type with details  
WITH complaint_details AS (
   SELECT DISTINCT
    complaint_type, 
    descriptor,
    descriptor_2 AS additional_details
      FROM {{ ref('stg_nyc_311') }}
    GROUP BY complaint_type, descriptor, descriptor_2
),

complaint_dimension AS (
   SELECT
       {{ dbt_utils.generate_surrogate_key([
           'complaint_type',
           'descriptor',
           'additional_details'
       ]) }} AS complaint_key,
      complaint_type,
      descriptor,
      additional_details
     FROM complaint_details
)

SELECT * FROM complaint_dimension