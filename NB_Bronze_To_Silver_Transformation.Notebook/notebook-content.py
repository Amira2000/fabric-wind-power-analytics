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

from pyspark.sql.functions import (col, round,dayofmonth, month, quarter, year,regexp_replace, substring, when)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Welcome to your new notebook
# Type here in the cell editor to add code!
bronze_table_path = "abfss://WindPowerAnalytics_OuakedAmira@onelake.dfs.fabric.microsoft.com/LH_Wind_Power_Bronze.lakehouse/Tables/dbo/wind_power_data"
df = spark.read.format("delta").load(bronze_table_path)
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Clean and enrich data
df_transformed = (
df
.withColumn("wind_speed", round(col("wind_speed"), 2))
.withColumn("energy_produced", round(col("energy_produced"), 2))
.withColumn("day", dayofmonth(col("date")))
.withColumn("month", month(col("date")))
.withColumn("quarter", quarter(col("date")))
.withColumn("year", year(col("date")))
.withColumn("time", regexp_replace(col("time"), "-", ":"))
.withColumn("hour_of_day", substring(col("time"), 1, 2).cast("int"))
.withColumn("minute_of_hour", substring(col("time"), 4, 2).cast("int"))
.withColumn("second_of_minute", substring(col("time"), 7,
2).cast("int"))
.withColumn(
"time_period",
when((col("hour_of_day") >= 5) & (col("hour_of_day") < 12),
"Morning")
.when((col("hour_of_day") >= 12) & (col("hour_of_day") < 17),
"Afternoon")
.when((col("hour_of_day") >= 17) & (col("hour_of_day") < 21),
"Evening")
.otherwise("Night")
)
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Path to the wind_power table in the Silver Lakehouse
silver_table_path = "abfss://WindPowerAnalytics_OuakedAmira@onelake.dfs.fabric.microsoft.com/LH_Wind_Power_Silver.Lakehouse/Tables/dbo/wind_power"
# Save the transformed table to the Silver Lakehouse
df_transformed.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(silver_table_path)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
