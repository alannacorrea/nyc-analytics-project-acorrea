  SELECT
     unique_key,
     created_date,
     complaint_type,
     borough
 FROM {{ source('raw', 'nyc_311_raw_data') }}
 LIMIT 10