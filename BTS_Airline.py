# Databricks notebook source
from pyspark.sql.functions import col, when 

volume = "/Volumes/airline_streaming/airline/airline"
checkpoint = f"{volume}/_checkpoints/flights"
schema = f"{volume}/_schema/flights"

df = (spark.readStream
      .format("cloudFiles")
      .option("cloudFiles.format", "csv")
      .option("cloudFiles.schemaLocation", schema)
      .option("cloudFiles.inferColumnTypes","false")
      .option("header","true")
      .load(volume)
      )

df = df.withColumn(
    "data_quality_issue",
    when(col("Origin").isNull(),"Missing Origin")
    .when(col("Dest").isNull(),"Missing Destination")
    .when(col("FlightDate").isNull(),"Missing FlightDate")
    .otherwise("OK")
)


df = df.withColumn(
    "delay_cause_missing",
    when(
        (col("ArrDel15").cast("double") == 1) &
        col("CarrierDelay").isNull() &
        col("WeatherDelay").isNull() &
        col("NASDelay").isNull() &
        col("SecurityDelay").isNull() &
        col("LateAircraftDelay").isNull(),
        True
     ).otherwise(False)
)
(df.writeStream
 .format("delta")
 .option("checkpointLocation", checkpoint)
 .trigger(availableNow=True)
 .toTable("flights")
 )

# COMMAND ----------

spark.table("flights").select("data_quality_issue").distinct().show()
spark.table("flights").select("delay_cause_missing").groupBy("delay_cause_missing").count().show()
spark.table("flights").filter(col("ArrDel15").cast("double") == 1).count()


# COMMAND ----------

