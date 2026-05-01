-- Location dimension documented within 311 data 

WITH all_locations AS (
   SELECT DISTINCT
        bbl_id,
        incident_zip,
        city, 
        community_board, 
        council_district, 
        police_precinct, 
        location_type
   FROM {{ ref('stg_nyc_311') }}

WHERE bbl_id IS NOT NULL

   GROUP BY 
        bbl_id,
        incident_zip,
        city, 
        community_board, 
        council_district, 
        police_precinct, 
        location_type
),

location_dimension AS (
   SELECT
       {{ dbt_utils.generate_surrogate_key([
        'bbl_id', 'incident_zip', 'city', 'community_board', 'council_district', 'police_precinct', 'location_type'
       ]) }} AS location_key,
        bbl_id,
        incident_zip,
        city, 
        community_board, 
        council_district, 
        police_precinct, 
        location_type
   FROM all_locations
)

SELECT * FROM location_dimension 