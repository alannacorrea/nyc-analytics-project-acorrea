-- Date dimension shared by both restaurant applications and 311 requests

WITH all_dates AS (
   -- Get dates (dates, no time included) from 311 requests
   SELECT DISTINCT CAST(created_date AS DATE) AS full_date
   FROM {{ ref('stg_nyc_311') }}
   WHERE created_date IS NOT NULL

   UNION DISTINCT

   -- Get dates from street pavement rating
   SELECT DISTINCT CAST(inspection_date AS DATE) AS full_date
   FROM {{ ref('stg_street_pavement_rating') }}
   WHERE inspection_date IS NOT NULL
),

date_dimension AS (
   SELECT
       {{ dbt_utils.generate_surrogate_key(['full_date']) }} AS date_key,

       full_date AS date_value,
       EXTRACT(DAY FROM full_date) AS day_of_month,
       FORMAT_DATE('%A', full_date) AS day_name,
       EXTRACT(DAYOFWEEK FROM full_date) AS day_of_week,
       EXTRACT(WEEK FROM full_date) AS week_of_year,
       EXTRACT(MONTH FROM full_date) AS month,
       FORMAT_DATE('%B', full_date) AS month_name,
       EXTRACT(QUARTER FROM full_date) AS quarter,
       EXTRACT(YEAR FROM full_date) AS year,
       EXTRACT(DAYOFWEEK FROM full_date) IN (1, 7) AS is_weekend,

--adding in logic for seasonality

  CASE WHEN EXTRACT(MONTH FROM full_date) IN (12, 1, 2) THEN 'Winter'
       WHEN EXTRACT(MONTH FROM full_date) IN (3, 4, 5) THEN 'Spring'
       WHEN EXTRACT(MONTH FROM your full_date) IN (6, 7, 8) THEN 'Summer'
       ELSE 'Fall' 
  END AS season

   FROM all_dates
)

SELECT * FROM date_dimension