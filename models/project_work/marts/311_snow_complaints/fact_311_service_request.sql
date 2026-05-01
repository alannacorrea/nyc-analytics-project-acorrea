WITH snow_complaints AS (
    SELECT *
    FROM {{ ref('stg_nyc_311') }} 
),

dim_borough AS (
    SELECT borough_key, borough FROM {{ ref('dim_borough') }}
),

dim_date AS (
    SELECT date_key, date_value 
    FROM {{ ref('dim_date_project') }}
),

dim_agency AS (
    SELECT agency_key, agency, agency_name FROM {{ ref('dim_agency') }}
),

dim_complaint AS (
    SELECT
        agency, 
        complaint_key,     
        complaint_type, 
        descriptor,
        additional_details
    FROM {{ ref('dim_complaint') }}
),

dim_location AS ( 
    SELECT
        location_key,
        bbl_id,
        incident_zip,
        city, 
        community_board, 
        council_district, 
        police_precinct, 
        location_type
    FROM {{ ref('dim_location_311') }}
),

final AS (
    SELECT
        -- Primary Key for the Fact Table
        {{ dbt_utils.generate_surrogate_key(['snow.request_id']) }} AS service_request_key,

        -- Measures and Attributes
        snow.request_id,
        snow.created_date,
        snow.closed_date,
        snow.status,
        snow.resolution_description,
        snow.method_of_submission,    

    -- Foreign Keys from Joins
        a.agency_key,
        b.borough_key,
        c.complaint_key,
        l.location_key,
        
        --Role-Playing Date Keys (Joined twice to dim_date below)
        dc.date_key AS created_date_key,
        dx.date_key AS closed_date_key

    FROM snow_complaints snow

    -- Lookup Agency Key
       LEFT JOIN dim_agency a
        ON snow.agency = a.agency
        AND snow.agency_name = a.agency_name
  
    -- Lookup Complaint Key
    LEFT JOIN dim_complaint c
        ON  snow.agency = c.agency
        AND snow.complaint_type = c.complaint_type
        AND snow.descriptor = c.descriptor
        AND snow.additional_details = c.additional_details

    -- Lookup Location Key
    LEFT JOIN dim_location l
        ON  snow.bbl_id            = l.bbl_id
        AND snow.incident_zip      = l.incident_zip
        AND snow.city              = l.city
        AND snow.community_board   = l.community_board
        AND snow.council_district  = l.council_district
        AND snow.police_precinct   = l.police_precinct
        AND snow.location_type = l.location_type 
  
    -- Lookup Borough Key
    LEFT JOIN dim_borough b 
        ON snow.borough = b.borough
    
    -- Lookup for the Created Date
    LEFT JOIN dim_date dc 
        ON snow.created_date = dc.date_value
        
    -- Lookup for the Closed Date
    LEFT JOIN dim_date dx 
        ON snow.closed_date = dx.date_value
)

SELECT * FROM final