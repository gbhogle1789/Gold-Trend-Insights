# Databricks notebook source
# DBTITLE 1,5 mins interval data
import pyspark.sql.functions as F
df = spark.read.format("csv").option("header", "true").load("/Volumes/workspace/default/gold_data/FX_XAUUSD, 5 - 3 UTC Month.csv")
df = df.filter("time >= '2024-07-01' and time < '2024-09-01'").select("time", "open", "close", "high", "low")
df = df.withColumn("open", F.col("open").cast("double"))\
    .withColumn("close", F.col("close").cast("double"))\
    .withColumn("high", F.col("high").cast("double"))\
    .withColumn("low", F.col("low").cast("double"))\
    .withColumn("round_hours",  F.date_trunc('hour', F.col("time")))\
    .withColumn("dayofweek", F.date_format("round_hours", 'EEEE') )\
    .withColumn("hour_min_time", F.date_format(F.col("time"),'HH:mm'))
    
df.createOrReplaceTempView("forex_raw")
df = spark.sql("select *,case when hour_min_time >='22:00' then date(date_add(time,1)) else date(time) end as actual_trade_cycle from forex_raw")
df.write.mode("append").options(overwriteSchema="true").format("delta").saveAsTable("forex_bronze")

# COMMAND ----------

# let’s add additional columns for hour , minute and  : done

# then aggregate delta for particular day between open till 11 AM  : done

# also add delta for last hour and high and low   

# Also add how many times it hit max and min every hour 




# COMMAND ----------



# COMMAND ----------

# MAGIC %sql
# MAGIC select min(actual_trade_cycle) from forex_agg_day1

# COMMAND ----------

df_process =df.withColumn("time2",  F.date_trunc('hour', F.col("time")))
df_process = df_process.withColumn("dayofweek", F.date_format("time2", 'EEEE') )\
    .withColumn("hour_of_day", F.hour(F.col("time")))
df_process.createOrReplaceTempView("forex_silver")

# COMMAND ----------

df_agg_day = spark.sql("""select distinct actual_trade_cycle, max(high) over(partition by actual_trade_cycle) as max_day_high,
min(low) over(partition by actual_trade_cycle) as min_day_low ,
first_value(open) over(partition by actual_trade_cycle) as Day_open,
last_value(close) over(partition by actual_trade_cycle) as Day_close,
round(Day_close - Day_open , 3) as Day_open_close_delta,
round(max_day_high - Day_open,3) as Day_high_open_delta,
round(Day_open - min_day_low,3) as Day_low_open_delta
from forex_bronze
order by actual_trade_cycle desc""")

df_agg_day.write.mode("append").saveAsTable("forex_agg_day1")

# COMMAND ----------

df_first_hr = spark.sql("""select distinct round_hours, from_utc_timestamp(round_hours, 'CST') as round_hours_cst,actual_trade_cycle,
first_value(open) over(partition by round_hours) as open_hour_value,
last_value(close) over(partition by round_hours) as close_hour_value,
max(high) over(partition by round_hours) as max_hour_high,
min(low) over(partition by round_hours) as min_hour_low,
round(max_hour_high - open_hour_value, 3) as high_open_hour_delta,
round(open_hour_value -min_hour_low , 3 )  as low_open_hour_delta,
round(open_hour_value - close_hour_value, 3) as open_close_delta,
round(open_hour_value - close_hour_value, 3) as open_close_hour_delta
from forex_bronze
where date_format(round_hours, 'HH:mm') = '22:00'""")
df_first_hr.write.mode("append").saveAsTable("forex_first_hr1")

# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct a.*, b.*   from forex_agg_day1  a left join forex_first_hr1 b
# MAGIC on a.actual_trade_cycle = b.actual_trade_cycle
# MAGIC order by a.actual_trade_cycle desc

# COMMAND ----------

# MAGIC %sql
# MAGIC drop table forex_first_hr1

# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct round_hours_cst,max_day_high,min_day_low, 
# MAGIC first_value(open) over(partition by round_hours) as open_hour_value,
# MAGIC last_value(close) over(partition by round_hours) as close_hour_value,
# MAGIC round(max_day_high - open_hour_value, 3) as high_open_hour_delta,
# MAGIC round(open_hour_value -min_day_low , 3 )  as low_open_hour_delta,
# MAGIC round(open_hour_value - close_hour_value, 3) as open_close_delta,
# MAGIC round(open_hour_value - close_hour_value, 3) as open_close_hour_delta
# MAGIC  from(
# MAGIC select *, from_utc_timestamp(round_hours, 'CST') as round_hours_cst,
# MAGIC max(high) over(partition by actual_trade_cycle) as max_day_high,
# MAGIC min(low) over(partition by actual_trade_cycle) as min_day_low
# MAGIC from forex_bronze)
# MAGIC where date_format(round_hours, 'HH:mm') = '22:00'

# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct round_hours_cst,open_day_value, close_day_value, max_hour_high, min_hour_low,high_open_hour_delta, low_open_hour_delta,open_close_delta, open_close_hour_delta
# MAGIC  from  forex_first_hr1 
# MAGIC order by round_hours_cst desc
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC select a.time, a.actual_trade_cycle,a.max_hour_high, b.max_day_high from forex_first_hr a join  forex_agg_day b 
# MAGIC on to_timestamp(a.round_hours) = to_timestamp(b.time)
# MAGIC

# COMMAND ----------

from pyspark.sql.window import Window
from pyspark.sql import functions as F
df_agg_first_hr = spark.sql("""select  a.actual_trade_cycle,a.max_hour_high, b.max_day_high,a.min_hour_low,  b.min_day_low, b.open_day_value, b.close_day_value from forex_first_hr a join  forex_agg_day b 
on to_timestamp(a.round_hours) = to_timestamp(b.time)""").alias("b")

window_spec = Window.orderBy("actual_trade_cycle")
df_first_hr_prev = df_agg_first_hr.distinct()\
.withColumn("prev_day_high", F.lag("max_day_high").over(window_spec))\
.withColumn("prev_day_low", F.lag("min_day_low").over(window_spec))\
.withColumn("prev_day_open", F.lag("open_day_value").over(window_spec))\
.withColumn("prev_day_close", F.lag("close_day_value").over(window_spec))
# .select("a.time", "a.actual_trade_cycle","a.max_hour_high", "b.max_day_high","prev_day_high","prev_day_low", "prev_day_open","prev_day_close" )

# COMMAND ----------

df_first_hr_prev.write.mode("overwrite").saveAsTable("forex_first_hr_withprev")

# COMMAND ----------

# MAGIC %sql
# MAGIC select distinct actual_trade_cycle, high_open_hour_delta, low_open_hour_delta, open_close_delta  from forex_first_hr1

# COMMAND ----------

# DBTITLE 1,everyday first hour  values with prev data
# MAGIC %sql
# MAGIC select actual_trade_cycle,high_open_Delta_cyle,open_day_value, min_day_low, low_open_Delta_cyle, open_close_delta_per_cycle  from forex_first_hr a left join forex_first_hr_withprev b 
# MAGIC on a.actual_trade_cycle = b.actual_trade_cycle
# MAGIC

# COMMAND ----------

# DBTITLE 1,# also add delta for last hour and high and low
df_last_hr_5min = spark.sql("""select *, 
first_value(open) over(partition by actual_trade_cycle) as open_day_value,
last_value(close) over(partition by actual_trade_cycle) as close_day_value,
max(high) over(partition by actual_trade_cycle) as max_day_high,
last_value(high) over(partition by actual_trade_cycle) as last_hour_high,
round(max_day_high - last_hour_high, 3) as last_hour_high_delta,
min(low) over(partition by actual_trade_cycle) as  min_day_low,
last_value(low) over(partition by actual_trade_cycle) as last_hour_low,
round(last_hour_low - min_day_low,3) as last_hour_low_delta,
round(max_day_high - min_day_low, 3) as hight_low_delta_per_day
 from forex_bronze
order by time""")
df_last_hr_5min.write.mode("overwrite").saveAsTable("forex_last_hr_5min")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from forex_last_hr_5min

# COMMAND ----------

# MAGIC %sql
# MAGIC -- select round_hours, sum(high_occur)  high_per_hour_occur
# MAGIC -- from(
# MAGIC select time, b.round_hours, b.actual_trade_cycle, open, high, high_per_hour,high_per_day, round((((high_per_hour - open_value_hour ) /high_per_day) * 100)/2,2)  deviation_percent ,
# MAGIC high_per_hour  - high as high_delta,
# MAGIC case when high_delta <= deviation_percent then 1 else 0 end high_occur
# MAGIC from forex_bronze b join forex_hourly h on b.round_hours=h.round_hours
# MAGIC where b.round_hours = '2024-09-01T22:00:00.000+00:00'
# MAGIC order by time 
# MAGIC -- group by round_hours

# COMMAND ----------

