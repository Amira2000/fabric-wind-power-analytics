# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "2f3f9258-0a08-43ff-917b-e450c2de4e2c",
# META       "default_lakehouse_name": "LH_Wind_Power_Bronze",
# META       "default_lakehouse_workspace_id": "d1aea760-fe6b-4e82-8724-88a548118c60",
# META       "known_lakehouses": [
# META         {
# META           "id": "2f3f9258-0a08-43ff-917b-e450c2de4e2c"
# META         },
# META         {
# META           "id": "011b86a2-ce71-4c17-888f-3711d32205c1"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC -- Create a temporary view of the Bronze wind_power table
# MAGIC CREATE OR REPLACE TEMP VIEW bronze_wind_power AS
# MAGIC SELECT *
# MAGIC FROM LH_Wind_Power_Bronze.dbo.wind_power_data;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Welcome to your new notebook
# Type here in the cell editor to add code!
CREATE OR REPLACE TEMP VIEW transformed_wind_power AS
SELECT
production_id,
date,
turbine_name,
capacity,
location_name,
latitude,
longitude,
region,
status,
responsible_department,
wind_direction,
ROUND(wind_speed, 2) AS wind_speed,
ROUND(energy_produced, 2) AS energy_produced,
DAY(date) AS day,
MONTH(date) AS month,
QUARTER(date) AS quarter,
YEAR(date) AS year,
REGEXP_REPLACE(time, '-', ':') AS time,
CAST(SUBSTRING(time, 1, 2) AS INT) AS hour_of_day,
CAST(SUBSTRING(time, 4, 2) AS INT) AS minute_of_hour,
CAST(SUBSTRING(time, 7, 2) AS INT) AS second_of_minute,
CASE
WHEN CAST(SUBSTRING(time, 1, 2) AS INT) BETWEEN 5 AND 11 THEN 'Morning'
WHEN CAST(SUBSTRING(time, 1, 2) AS INT) BETWEEN 12 AND 16 THEN 'Afternoon'
WHEN CAST(SUBSTRING(time, 1, 2) AS INT) BETWEEN 17 AND 20 THEN 'Evening'
ELSE 'Night'
END AS time_period
FROM wind_power_data;

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
