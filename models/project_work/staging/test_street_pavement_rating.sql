 SELECT
     oftcode,
     inspection,
     boroughname,
     systemrating
 FROM {{ source('raw', 'street_pavement_data') }}
 LIMIT 10
