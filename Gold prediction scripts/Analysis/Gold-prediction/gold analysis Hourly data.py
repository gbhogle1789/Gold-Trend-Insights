# Databricks notebook source


import pyspark.sql.functions as F
df = spark.read.format("csv").option("header", "true").load("/Volumes/workspace/default/gold_data/FX_XAUUSD, 60-UTC.csv")
df = df.filter("time >= '2024-09-30T22:00:00Z' and time <= '2024-10-23'").select("time", "open", "close", "high", "low")
df = df.withColumn("open", F.col("open").cast("double"))\
    .withColumn("close", F.col("close").cast("double"))\
    .withColumn("high", F.col("high").cast("double"))\
    .withColumn("low", F.col("low").cast("double"))\
    .withColumn("dayofweek", F.date_format("time", 'EEEE') )\
    .withColumn("hour_min_time", F.date_format(F.col("time"),'HH:mm'))
    # .withColumn("round_hours",  F.date_trunc('hour', F.col("time")))\
    # .withColumn("hour_time", F.hour(F.col("time")))\
    # .withColumn("min_time", F.minute(F.col("time")))\
    
df.createOrReplaceTempView("forex_raw")
df = spark.sql("select *,case when hour_min_time >='22:00' then date(date_add(time,1)) else date(time) end as actual_trade_cycle from forex_raw")
df.write.mode("overwrite").options(overwriteSchema="true").format("delta").saveAsTable("forex_bronze_hourly")

# COMMAND ----------

#10/22/2024
# let’s add additional columns for hour , minute and  : done

# then aggregate delta for particular day between open till 11 AM  : done

# also add delta for last hour and high and low   

# Also add how many times it hit max and min every hour 




# COMMAND ----------

# DBTITLE 1,2. aggregate delta for particular day between open till 11 AM

#then aggregate delta for particular day between open till 11 AM  
df_agg = spark.sql("""select actual_trade_cycle, 
 round(close - open,3) as open_close_delta
,round(high - low, 3) as high_low_delta
,round(high -open,3) as high_open_delta
from forex_bronze_hourly
--where (hour_min_time <'11:00' or hour_min_time >='22:00')

order by actual_trade_cycle""")

df_agg.write.mode("overwrite").option("mergeSchema", "true").saveAsTable("forex_agg")
  



# COMMAND ----------

# MAGIC %sql
# MAGIC select * from forex_agg

# COMMAND ----------

# DBTITLE 1,2. aggregate delta for particular day between open till 11 AM
# MAGIC %sql
# MAGIC select * from
# MAGIC  forex_agg_11

# COMMAND ----------

# DBTITLE 1,2. aggregate delta for particular day between open till 11 AM
# MAGIC %sql
# MAGIC select * from
# MAGIC  forex_agg_11

# COMMAND ----------

df_process =df.withColumn("time2",  F.date_trunc('hour', F.col("time")))
df_process = df_process.withColumn("dayofweek", F.date_format("time2", 'EEEE') )\
    .withColumn("hour_of_day", F.hour(F.col("time")))
df_process.createOrReplaceTempView("forex_silver")

# COMMAND ----------

# DBTITLE 1,# also add delta for last hour and high and low
df_last_hr = spark.sql("""select *, 
first_value(open) over(partition by actual_trade_cycle) as open_day_value,
last_value(close) over(partition by actual_trade_cycle) as close_day_value,
max(high) over(partition by actual_trade_cycle) as max_day_high,
last_value(high) over(partition by actual_trade_cycle) as last_hour_high,
round(max_day_high - last_hour_high, 3) as last_hour_high_delta,
min(low) over(partition by actual_trade_cycle) as  min_day_low,
last_value(low) over(partition by actual_trade_cycle) as last_hour_low,
round(last_hour_low - min_day_low,3) as last_hour_low_delta,
round(max_day_high - min_day_low, 3) as hight_low_delta_per_day
 from forex_bronze_hourly
order by time""")
df_last_hr.write.mode("overwrite").saveAsTable("forex_last_hr")

# COMMAND ----------

# DBTITLE 1,With previous day data
from pyspark.sql.window import Window
from pyspark.sql import functions as F
window_spec = Window.orderBy("actual_trade_cycle")
df_last_hr_prev = df_last_hr.select("actual_trade_cycle", "open_day_value","close_day_value", "max_day_high","last_hour_high", "min_day_low", "last_hour_low").distinct()\
.withColumn("prev_day_high", F.lag("max_day_high").over(window_spec))\
.withColumn("prev_day_low", F.lag("min_day_low").over(window_spec))\
.withColumn("prev_day_open", F.lag("open_day_value").over(window_spec))\
.withColumn("prev_day_close", F.lag("close_day_value").over(window_spec))

# COMMAND ----------

# DBTITLE 1,with Previous_day values
df_last_hr_prev.write.mode("overwrite").saveAsTable("forex_last_hr_prev")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from forex_last_hr_prev

# COMMAND ----------

# DBTITLE 1,New start
df_agg = spark.sql("""select *, 
first_value(open) over(partition by actual_trade_cycle) as open_day_value,
last_value(close) over(partition by actual_trade_cycle) as close_day_value,
max(high) over(partition by actual_trade_cycle) as max_day_high,
min(low) over(partition by actual_trade_cycle) as  min_day_low,
round(close_day_value - open_day_value,3) as open_close_delta_per_cycle,
round(max_day_high - min_day_low, 3) as high_low_delta_per_cycle,
round(max_day_high - open_day_value, 3) as high_open_Delta_cyle,
round(open_day_value -min_day_low, 3) as low_open_Delta_cyle
 from forex_bronze_hourly
order by time""")
df_agg.write.mode("overwrite").saveAsTable("forex_agg1")

# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct actual_trade_cycle,high_open_Delta_cyle,open_day_value, min_day_low, low_open_Delta_cyle,open_close_delta_per_cycle   from forex_agg1
# MAGIC
# MAGIC order by actual_trade_cycle desc

# COMMAND ----------

df_agg_day = df_agg\
.distinct()\
.withColumn("highdeviation10%",  F.round(F.col("high_open_Delta_cyle") *0.1,3)) \
.withColumn("highdeviation20%",  F.round(F.col("high_open_Delta_cyle") *0.2,3)) \
.withColumn("lowdeviation10%",   F.round(F.col("low_open_Delta_cyle") *0.1 ,3))\
.withColumn("lowdeviation20%",   F.round(F.col("low_open_Delta_cyle") *0.2 ,3))

df_agg_day.write.mode("overwrite").saveAsTable("forex_agg_day")

# COMMAND ----------

display(df_agg_day)

# COMMAND ----------

df_occ_1 = spark.sql("""select actual_trade_cycle,high, max_day_high,open,
round(max_day_high - high,3) as high_delta,`highdeviation10%`,`highdeviation20%`,
case when high_delta <= `highdeviation10%` then 1 else 0 end as high_occur_10,
case when high_delta <= `highdeviation20%` then 1 else 0 end as high_occur_20,
low, min_day_low, `lowdeviation10%`,`lowdeviation20%`,
round(low - min_day_low, 3) as low_delta,
case when low_delta <= `lowdeviation10%` then 1 else 0 end as low_occur_10,
case when low_delta <= `lowdeviation20%` then 1 else 0 end as low_occur_20
 from 
forex_agg_day""")
df_occ_1.write.mode("overwrite").saveAsTable("forex_high_low_occ1")

# COMMAND ----------

# MAGIC %sql 
# MAGIC select * from forex_high_low_occ1

# COMMAND ----------

# DBTITLE 1,High low occurances
df_occ_final = spark.sql("""select actual_trade_cycle, max_day_high, sum(high_occur_10) as high_value_hit_10, sum(high_occur_20) as high_value_hit_20,
sum(low_occur_10) as low_value_hit_10, sum(low_occur_20) as low_value_hit_20 from
forex_high_low_occ1
group by 1,2
order by 1""")
df_occ_final.write.mode("overwrite").saveAsTable("forex_high_low_occ_final")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from forex_high_low_occ_final