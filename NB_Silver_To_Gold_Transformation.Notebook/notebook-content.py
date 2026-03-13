# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "8f57801e-9907-455f-986e-0f4753deb6cd",
# META       "default_lakehouse_name": "LH_Wind_Power_Gold",
# META       "default_lakehouse_workspace_id": "d1aea760-fe6b-4e82-8724-88a548118c60",
# META       "known_lakehouses": [
# META         {
# META           "id": "8f57801e-9907-455f-986e-0f4753deb6cd"
# META         }
# META       ]
# META     },
# META     "warehouse": {
# META       "default_warehouse": "3c2b8aaa-d29a-473a-bab1-5c51d408ba8f",
# META       "known_warehouses": [
# META         {
# META           "id": "3c2b8aaa-d29a-473a-bab1-5c51d408ba8f",
# META           "type": "Lakewarehouse"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# Welcome to your new notebook
# Type here in the cell editor to add code!


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.window import Window
from pyspark.sql.functions import row_number

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Path to the table in the Silver Lakehouse
silver_table_path = "abfss://WindPowerAnalytics_OuakedAmira@onelake.dfs.fabric.microsoft.com/LH_Wind_Power_Silver.lakehouse/Tables/dbo/wind_power"
# Load the table into a DataFrame
df = spark.read.format("delta").load(silver_table_path)
# Display df
display(df)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Create the Date Dimension Table
date_dim = (
 df.select(
 "date",
 "day",
 "month",
 "quarter",
 "year",
 )
 .distinct()
 .withColumnRenamed("date", "date_id")
)
# Display date_dim
display(date_dim)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Create the Time Dimension Table
time_dim = (
 df.select(
 "time",
 "hour_of_day",
 "minute_of_hour",
 "second_of_minute",
 "time_period",
 )
 .distinct()
 .withColumnRenamed("time", "time_id")
)
# Display time_dim
display(time_dim)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

turbine_dim = (
 df.select(
 "turbine_name",
 "capacity",
 "location_name",
 "latitude",
 "longitude",
 "region",
 )
 .distinct()
 .withColumn(
 "turbine_id",
 row_number().over(Window.orderBy("turbine_name", "capacity",
"location_name", "latitude", "longitude", "region")),
 )
)
# Display turbine_dim
display(turbine_dim)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Create the Operational Status Dimension Table
operational_status_dim = (
 df.select(
 "status",
 "responsible_department",
 )
 .distinct()
 .withColumn(
 "status_id",
 row_number().over(Window.orderBy("status", "responsible_department")),
 )
)
# Display operational_status_dim
display(operational_status_dim)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Join the dimension tables to the original DataFrame
df = (
 df.join(
 turbine_dim,
 ["turbine_name", "capacity", "location_name", "latitude", "longitude",
"region"],
 "left",
 )
 .join(
 operational_status_dim,
 ["status", "responsible_department"],
 "left",
 )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Create the Fact table
fact_table = (
 df.select(
 "production_id",
 "date",
 "time",
 "turbine_id",
 "status_id",
 "wind_direction",
 "wind_speed",
 "energy_produced",
 )
 .withColumnRenamed("date", "date_id")
 .withColumnRenamed("time", "time_id")
)
# Display fact_table
display(fact_table)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

gold_date_dim_path = "abfss://WindPowerAnalytics_OuakedAmira@onelake.dfs.fabric.microsoft.com/LH_Wind_Power_Gold.Lakehouse/Tables/dbo/dim_date"
gold_time_dim_path = "abfss://WindPowerAnalytics_OuakedAmira@onelake.dfs.fabric.microsoft.com/LH_Wind_Power_Gold.Lakehouse/Tables/dbo/dim_time"
gold_turbine_dim_path = "abfss://WindPowerAnalytics_OuakedAmira@onelake.dfs.fabric.microsoft.com/LH_Wind_Power_Gold.Lakehouse/Tables/dbo/dim_turbine"
gold_operational_status_dim_path = "abfss://WindPowerAnalytics_OuakedAmira@onelake.dfs.fabric.microsoft.com/LH_Wind_Power_Gold.Lakehouse/Tables/dbo/dim_operational_status"
gold_fact_table_path = "abfss://WindPowerAnalytics_OuakedAmira@onelake.dfs.fabric.microsoft.com/LH_Wind_Power_Gold.Lakehouse/Tables/dbo/FactWindPower"


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Save the tables in the Gold Lakehouse
date_dim.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(gold_date_dim_path)
time_dim.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(gold_time_dim_path)
turbine_dim.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(gold_turbine_dim_path)
operational_status_dim.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(gold_operational_status_dim_path)
fact_table.write.format("delta").mode("overwrite").option("overwriteSchema","true").save(gold_fact_table_path)



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
