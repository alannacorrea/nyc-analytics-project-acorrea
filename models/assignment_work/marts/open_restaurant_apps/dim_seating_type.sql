-- Seating type dimension for open restaurant seating applications
WITH seating_types AS (
   SELECT DISTINCT
       seating_interest_sidewalk AS seating_interest,
     CAST(
        CASE WHEN approved_for_sidewalk_seating = 'yes' THEN TRUE
        WHEN approved_for_sidewalk_seating = 'no' THEN FALSE
        END AS BOOLEAN) AS approved_for_sidewalk,

    CAST(
        CASE WHEN approved_for_roadway_seating = "yes" THEN TRUE
        WHEN approved_for_roadway_seating = 'no' THEN FALSE
        END AS BOOLEAN) AS approved_for_roadway, 

   FROM {{ ref('stg_nyc_open_restaurant_apps') }}
    WHERE seating_interest_sidewalk IS NOT NULL
),
seating_dimension AS (
   SELECT
       {{ dbt_utils.generate_surrogate_key([
           'seating_interest',
           'approved_for_sidewalk',
           'approved_for_roadway'
       ]) }} AS seating_type_key,

        seating_interest, 
        approved_for_sidewalk, 
        approved_for_roadway
   FROM seating_types
)

SELECT * FROM seating_dimension