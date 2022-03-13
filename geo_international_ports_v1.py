import pandas as pd
from google.cloud import bigquery
import os

os.environ["GOOGLE_APPLICATION_CREDENTIALS"]="eloquent-ratio-344005-f412f40b4fd9.json"

gcp_project = 'eloquent-ratio-344005'
bq_dataset = ' bigquery-public-data:geo_international_ports.world_port_index'

client = bigquery.Client(project = gcp_project)


# Perform a query.
QUERY = """
    SELECT * FROM `bigquery-public-data.geo_international_ports.world_port_index` 
    LIMIT 10
    """
query_job = client.query(QUERY)  # API request
rows = query_job.result()  # Waits for query to finish

for row in rows:
    print(row.country)



#Q1

QUERY = """
WITH jurong_island AS (
    SELECT port_name, country, port_latitude, port_longitude 
FROM `bigquery-public-data.geo_international_ports.world_port_index`
WHERE country = 'SG' and port_name = 'JURONG ISLAND'
), distance_to_jurong_island AS (
SELECT 
    ST_GEOGPOINT(port1.port_longitude, port1.port_latitude) AS port1,
    port1.port_name AS start_port_name,
    ST_GEOGPOINT(jurong_island_port.port_longitude, jurong_island_port.port_latitude) AS port2,
    jurong_island_port.port_name AS end_port_name,
    ST_DISTANCE(ST_GEOGPOINT(port1.port_longitude, port1.port_latitude), 
    ST_GEOGPOINT(jurong_island_port.port_longitude, jurong_island_port.port_latitude)) AS dist
FROM `bigquery-public-data.geo_international_ports.world_port_index` AS port1, 
  jurong_island AS jurong_island_port

)
SELECT port_name
FROM (
    SELECT start_port_name AS port_name, dist
    FROM distance_to_jurong_island
    WHERE dist <> 0)
ORDER BY dist ASC
LIMIT 5
"""

query_job = client.query(QUERY)  # API request
rows = query_job.result()  # Waits for query to finish

for row in rows:
    print(row)



#Q2

QUERY = """
SELECT country, COUNT(cargo_wharf) AS port_count
FROM (
    SELECT country, cargo_wharf 
    FROM`bigquery-public-data.geo_international_ports.world_port_index`
    WHERE cargo_wharf = TRUE
)
GROUP BY country
ORDER BY COUNT(cargo_wharf) DESC
LIMIT 1;
"""
query_job = client.query(QUERY)  # API request
rows = query_job.result()  # Waits for query to finish

for row in rows:
    print(row)




#Q3

QUERY = """
WITH distance_to_distressed AS (
SELECT 
    country, port_name, port_latitude, port_longitude,
    provisions, water, fuel_oil, diesel, 
    port_with_replenishment.port_name AS start_port_name,
    ST_DISTANCE(ST_GEOGPOINT(port_with_replenishment.port_longitude, port_with_replenishment.port_latitude), 
    ST_GEOGPOINT( -38.706256,  32.610982)) AS dist_to_distressed
FROM (
    SELECT * 
    FROM  `bigquery-public-data.geo_international_ports.world_port_index` port
    WHERE provisions = TRUE and water = TRUE and fuel_oil = TRUE and diesel = TRUE
) AS port_with_replenishment
)
SELECT country, port_name, port_latitude, port_longitude
FROM distance_to_distressed
ORDER BY dist_to_distressed ASC
LIMIT 1
"""
query_job = client.query(QUERY)  # API request
rows = query_job.result()  # Waits for query to finish

for row in rows:
    print(row)