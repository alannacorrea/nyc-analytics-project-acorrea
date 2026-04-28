  SELECT
     unique_key,
     created_date,
     complaint_type,
     borough
 FROM {{ source('raw_project_data_311', 'nyc_311_raw_data') }}
 LIMIT 10