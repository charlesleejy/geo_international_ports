# geo_international_ports

The geo_international_ports.py script reads bigquery-public-data from BigQuery and generate output to BigQuery table to answer the following questions.

1. What are the 5 nearest ports to Singapore's JURONG ISLAND port?
(country = 'SG', port_name = 'JURONG ISLAND')
Your answer should include the columns port_name and distance_in_meters only.
2. Which country has the largest number of ports with a cargo_wharf? Your answer should include
the columns country and port_count only.
3. You receive a distress call from the middle of the North Atlantic Ocean. The person on the line gave you
a coordinates of lat: 32.610982, long: -38.706256 and asked for the nearest port with
provisions, water, fuel_oil and diesel. Your answer should include the columns
country, port_name, port_latitude and port_longitude only.


Steps to generate output
1. Download the key from IAM of your personal Google Cloud Project  
2. Set up the environment variable by running command on Windows: set GOOGLE_APPLICATION_CREDENTIALS=KEY_PATH
3. Define the project_id, credential_file and the target tables for each of the question in the geo_international_ports.py
4. run geo_international_ports.py
