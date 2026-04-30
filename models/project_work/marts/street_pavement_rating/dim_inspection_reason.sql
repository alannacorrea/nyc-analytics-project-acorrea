--  dimension for streets not receiving a rating
WITH non_rating_reason AS (
   SELECT DISTINCT
        CASE WHEN nonratingreason IN ('Other', 'Does Not Exist') OR nonratingreason IS NULL THEN 'Not Available'
        ELSE nonratingreason
        END AS nonratingreason,
   FROM {{ ref('stg_street_pavement_rating') }}
),

inspection_dimension AS (
   SELECT
       {{ dbt_utils.generate_surrogate_key([
           'nonratingreason'
       ]) }} AS inspection_reason_key,
      nonratingreason,
     FROM non_rating_reason
)

SELECT * FROM inspection_dimension