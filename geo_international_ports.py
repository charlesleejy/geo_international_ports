import pandas as pd
from google.cloud import bigquery
from google.oauth2.service_account import Credentials
import os

os.environ["GOOGLE_APPLICATION_CREDENTIALS"]="eloquent-ratio-344005-f412f40b4fd9.json"

project_id = "eloquent-ratio-344005"
client = bigquery.Client(project = project_id)

credential_file = "eloquent-ratio-344005-f412f40b4fd9.json"
credential = Credentials.from_service_account_file(credential_file)
job_location = "australia-southeast1"

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

query_job = client.query(QUERY)
result  = query_job.to_dataframe()

target_table = "question1.output"

# Save Pandas dataframe to BQ
result.to_gbq(target_table, project_id=project_id, if_exists='replace',
          location=job_location, progress_bar=True, credentials=credential)



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
query_job = client.query(QUERY)
result  = query_job.to_dataframe()

# Define target table in BQ
target_table = "question2.output"

# Save Pandas dataframe to BQ
result.to_gbq(target_table, project_id=project_id, if_exists='replace',
          location=job_location, progress_bar=True, credentials=credential)


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
query_job = client.query(QUERY)
result  = query_job.to_dataframe()

# Define target table in BQ
target_table = "question3.output"

# Save Pandas dataframe to BQ
result.to_gbq(target_table, project_id=project_id, if_exists='replace',
          location=job_location, progress_bar=True, credentials=credential)

