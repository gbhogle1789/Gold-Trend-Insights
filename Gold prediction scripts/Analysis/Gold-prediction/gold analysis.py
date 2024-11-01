# Databricks notebook source
# DBTITLE 1,Jul 2024 data filter
import pyspark.sql.functions as F
df = spark.read.format("csv").option("header", "true").load("/Volumes/workspace/default/gold_data/FOREXCOM_XAUUSD 3 Months - Copy.csv")
df = df.filter("time > '2024-09-01' and time <= '2024-10-19'").select("time", "open", "close", "high", "low")
df = df.withColumn("open", F.col("open").cast("double"))\
    .withColumn("close", F.col("close").cast("double"))\
    .withColumn("high", F.col("high").cast("double"))\
    .withColumn("low", F.col("low").cast("double"))\
    .withColumn("round_hours",  F.date_trunc('hour', F.col("time")))\
    .withColumn("dayofweek", F.date_format("round_hours", 'EEEE') )\
    .withColumn("hour_min_time", F.date_format(F.col("time"),'HH:mm').cast("timestamp","HH:mm"))\
    .withColumn("hour_time", F.hour(F.col("time")))\
    .withColumn("min_time", F.minute(F.col("time")))

df.write.mode("overwrite").format("delta").saveAsTable("forex_bronze")

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table forex_bronze

# COMMAND ----------

df_process =df.withColumn("round_hours",  F.date_trunc('hour', F.col("time")))
df_process = df_process.withColumn("dayofweek", F.date_format("round_hours", 'EEEE') )\
    .withColumn("hour_min_time", F.date_format(F.col("time"),'HH:mm'))\
    .withColumn("hour_time", F.hour(F.col("time")))\
    .withColumn("min_time", F.minute(F.col("time")))
df_process.createOrReplaceTempView("forex_bronze")
df_process.write.mode("overwrite").options(overwriteSchema="true").format("delta").saveAsTable("forex_silver")

# COMMAND ----------



# COMMAND ----------

# DBTITLE 1,Hourly data
df_hourly = spark.sql("""
select  distinct time2 as hour_timestamp, date_format(time2, 'EEEE') as dayofweek, 
date_format(time2,'HH:mm') hour_time,
cast(first_value(open) over (partition by time2 order by time)as double) as hour_open, 
cast(last_value(close) over (partition by time2)as double) as hour_close,
cast(max(high) over (partition by time2)as double) as hour_high,
cast(min(low) over (partition by time2)as double) as hour_low
from forex_silver a""")
# df_hourly.createOrReplaceTempView("forex_hourly")
df_hourly.write.mode("overwrite").options(overwriteSchema="true").format("delta").saveAsTable("forex_hourly")

# COMMAND ----------

# DBTITLE 1,Hourly data with DayOfWeek
# MAGIC %sql
# MAGIC select  distinct time2 as hour_timestamp, date_format(time2, 'EEEE') as dayofweek, 
# MAGIC date_format(time2,'HH:mm') hour_time,
# MAGIC cast(first_value(open) over (partition by time2 order by time)as double) as hour_open, 
# MAGIC cast(last_value(close) over (partition by time2)as double) as hour_close,
# MAGIC cast(max(high) over (partition by time2)as double) as hour_high,
# MAGIC cast(min(low) over (partition by time2)as double) as hour_low,
# MAGIC case when hour_time = '22:00' then round((hour_high - hour_open),2) else 0 end as first_1_hour_delta --comment 'Delta between 1 hour high and open'
# MAGIC from forex_silver a
# MAGIC where date_format(time2,'HH:mm') in('20:00','22:00')
# MAGIC
# MAGIC ---start time : high - open delta 22:00 UTC
# MAGIC --find % for above delta
# MAGIC -- occurances for high value  and low value 

# COMMAND ----------

# DBTITLE 1,for every 10 pm data
# MAGIC
# MAGIC %sql
# MAGIC -- can be filter with any hour of the day
# MAGIC select *
# MAGIC   from forex_hourly 
# MAGIC   where date_part('HOUR', time2) = 22

# COMMAND ----------

# MAGIC %sql
# MAGIC
# MAGIC select *, date(time2) as curr_date
# MAGIC from forex_hourly
# MAGIC where --date_part('HOUR' ,time2) between 11 and 16 and
# MAGIC  date(time2) between '2024-10-01' and '2024-10-18'
# MAGIC