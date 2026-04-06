# Databricks notebook source
# MAGIC %md
# MAGIC NYC Taxi ETL Pipeline | 
# MAGIC Bronze → Silver → Gold Architecture | 
# MAGIC Built using PySpark on Databricks

# COMMAND ----------

# MAGIC %sql ALTER TABLE dev.default.yellow_tripdata_2026_01 
# MAGIC RENAME TO dev.default.nyx_taxi_bronze

# COMMAND ----------

from pyspark.sql.functions import *
df = spark.read.table("dev.default.nyx_taxi_bronze")

# COMMAND ----------

# Step 1: Data Cleaning (Bronze → Silver)

# Remove invalid and null records to ensure data quality
df_clean = df.filter(                                                                   
    (col("trip_distance")>0)&
    (col("fare_amount")>0)&
    col("tpep_pickup_datetime").isNotNull()&
    col("tpep_dropoff_datetime").isNotNull()
)


# Feature Engineering
# Caluculate trip durtation in minutes using timestamp difference
df_clean = df_clean.withColumn(                                                        
    "trip_duration_minutes",
    round((col("tpep_dropoff_datetime").cast("long") - col("tpep_pickup_datetime").cast("long"))/60,2)
)

#Remove unrealistic trips(ex: extremely long durations)
dF_clean = df_clean.filter(                                                             
    (col("trip_duration_minutes")>0)&
    (col("trip_duration_minutes")<300))


# Save cleaned data to Silver Layer
df_clean.write.mode("overwrite").saveAsTable("dev.default.nyx_taxi_silver")

# COMMAND ----------

df_silver = spark.read.table("dev.default.nyx_taxi_silver")

# Step 2: Aggregation (Silver → Gold)
# Generate business metrics for analysis
df_agg = df_silver.groupBy(
    to_date("tpep_pickup_datetime").alias("pickup_date")
).agg(
    sum("fare_amount").alias("total_revenue"),
    avg("trip_distance").alias("avg_distance"),
    count("*").alias("total_trips")
)

# COMMAND ----------

# Save Aggregated data as partitioned gold table for performance optimization

df_agg.write.mode("overwrite").saveAsTable("dev.default.nyc_taxi_gold")

# COMMAND ----------

# df_agg.write \
#     .mode("overwrite") \
#     .partitionBy("pickup_date") \
#     .saveAsTable("dev.default.nyc_taxi_gold")

# COMMAND ----------

# MAGIC %sql OPTIMIZE dev.default.nyc_taxi_gold
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT pickup_date, total_revenue
# MAGIC FROM dev.default.nyc_taxi_gold
# MAGIC ORDER BY pickup_date

# COMMAND ----------

from delta.tables import DeltaTable

gold_table = "dev.default.nyc_taxi_gold_partitioned"

try:

    delta_table = DeltaTable.forName(spark, gold_table)

    delta_table.alias("target".merge(
        df.agg.alias("source"),
        "target.pickup_date = source.pickup_date"
    ).whenMatchedUpdateAll() \
     .whenNotMatchedInsertAll() \
     .execute()
    )
except:
    df_agg.write.mode("overwrite").saveAsTable(gold_table)
                      

# COMMAND ----------

# DBTITLE 1,al done
a = spark.read.table(gold_table)
a.display()

# COMMAND ----------

