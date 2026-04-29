WITH pavement_ratings AS (
    SELECT * FROM {{ ref('stg_street_pavement_rating') }} -- staging model
),

dim_street_segment AS (
    SELECT street_dimension_key, oftcode FROM {{ ref('dim_street_segment') }}
),

dim_borough AS (
    SELECT borough_key, borough FROM {{ ref('dim_borough') }}
),

dim_date AS (
    SELECT date_key, date_value FROM {{ ref('dim_date_project') }}
),

dim_inspection_reason AS (
    SELECT inspection_reason_key, nonratingreason FROM {{ ref('dim_inspection_reason') }}
),

final AS (
    SELECT
        -- Primary Key for the Fact Table
        {{ dbt_utils.generate_surrogate_key([
            'pr.oftcode', 
            'pr.inspection_date'
        ]) }} AS pavement_fact_key,

        -- Measures and Attributes
        pr.systemrating,
        pr.locationgeometry_stlength,

        -- Foreign Keys
        s.street_segment_key,
        b.borough_key,
        d.date_key AS inspection_date_key,
        i.inspection_reason_key

    FROM pavement_ratings pr

    -- Lookup Street Segment Key
    LEFT JOIN dim_street_segment s 
        ON pr.oftcode = s.oftcode
    
    -- Lookup Borough Key
    LEFT JOIN dim_borough b 
        ON pr.borough = b.borough
    
    -- Lookup Inspection Date Key
    LEFT JOIN dim_date d 
        ON CAST(pr.inspection_date AS DATE) = d.date_value
    
    -- Lookup Inspection Reason Key
    LEFT JOIN dim_inspection_reason i 
        ON pr.nonratingreason = i.nonratingreason
)

SELECT * FROM final