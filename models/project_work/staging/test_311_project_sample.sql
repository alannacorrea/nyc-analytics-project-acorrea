 SELECT
     oftcode,
     inspection,
     boroughname,
     systemrating
 FROM {{ source('raw_pavement_rating', 'street_pavement_data') }}
 LIMIT 10