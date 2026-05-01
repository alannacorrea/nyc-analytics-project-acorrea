-- Borough dimension shared by both street pavement ratings and 311 service reqs

WITH borough_union AS (
   -- Get locations from 311 requests
   SELECT DISTINCT
        CAST (borough AS STRING) AS borough,
   FROM {{ ref('stg_nyc_311') }}
   WHERE borough IS NOT NULL

   UNION DISTINCT

   -- Get locations from street pavement rating
   SELECT DISTINCT
        CAST (borough AS STRING) AS borough,
   FROM {{ ref('stg_street_pavement_rating') }}
   WHERE borough IS NOT NULL
),

borough_dimension AS (
   SELECT
       {{ dbt_utils.generate_surrogate_key(['borough']) }} AS borough_key,
       borough,
   FROM borough_union
)

SELECT * FROM borough_dimension 