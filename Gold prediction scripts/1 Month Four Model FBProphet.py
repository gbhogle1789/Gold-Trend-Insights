# Databricks notebook source
!pip install prophet

# COMMAND ----------

import pandas as pd
import numpy as np
from datetime import datetime
from prophet import Prophet
import matplotlib.pyplot as plt
from sklearn.metrics import mean_absolute_error, mean_squared_error
import warnings
import pyspark.sql.functions as F
from dateutil.relativedelta import relativedelta
from datetime import datetime, timedelta


# COMMAND ----------

today = datetime.now() + timedelta(days=1)
today = today.strftime('%Y-%m-%d')
one_month_ago = (datetime.now() - relativedelta(months=1)).strftime('%Y-%m-%d')

# COMMAND ----------

print(today)

# COMMAND ----------

# DBTITLE 1,Open model
ticker = "GC=F"
minutes = 600
df = spark.sql("select * from forexcom_xauusd_3_months")
df = df.filter(f"time > '{one_month_ago}' and time < '{today}'").select("time", "open")
df = df.withColumn("open", F.col("open").cast("double"))\

bars = df.toPandas()
stock = pd.DataFrame(columns = ["time", "open"])
stock = bars
stock = stock.reset_index()
test_size = 100
data_train = stock.iloc[:-test_size, :]
data_test = stock.iloc[-test_size:, :]
df = data_train.reset_index()
df=df.rename(columns={'time':'ds', 'open':'y'})
df['ds'] = pd.to_datetime(df['ds'])
model = Prophet(weekly_seasonality=False)
model.add_seasonality(name='weekly', period=7, fourier_order=10)
model.fit(df)


df_test = data_test.reset_index()
df_test=df_test.rename(columns={'time':'ds', 'open':'y'})
df_test['ds'] = pd.to_datetime(df_test['ds'])

future_open = model.make_future_dataframe(minutes, freq = 'min')

future_open['minute'] = future_open['ds'].dt.minute
future_open['day_of_week'] = future_open['ds'].dt.dayofweek
future_open = future_open[(future_open['minute']%5==0)]
future_open = future_open[future_open['day_of_week'] < 5]

forecast_open = model.predict(future_open)
fig = model.plot_components(forecast_open)

# COMMAND ----------

fig1 = model.plot(forecast_open)

# COMMAND ----------

# forecast_open

# COMMAND ----------

# DBTITLE 1,high model
ticker = "GC=F"
minutes = 600
df = spark.sql("select * from forexcom_xauusd_3_months")
df = df.filter(f"time > '{one_month_ago}' and time < '{today}'").select("time", "high")
df = df.withColumn("high", F.col("high").cast("double"))\

bars = df.toPandas()
stock = pd.DataFrame(columns = ["time", "high"])
stock = bars
stock = stock.reset_index()
test_size = 100
data_train = stock.iloc[:-test_size, :]
data_test = stock.iloc[-test_size:, :]
df = data_train.reset_index()
df=df.rename(columns={'time':'ds', 'high':'y'})
df['ds'] = pd.to_datetime(df['ds'])
model = Prophet(weekly_seasonality=False)
model.add_seasonality(name='weekly', period=7, fourier_order=10)
model.fit(df)


df_test = data_test.reset_index()
df_test=df_test.rename(columns={'time':'ds', 'high':'y'})
df_test['ds'] = pd.to_datetime(df_test['ds'])

future_high = model.make_future_dataframe(minutes, freq = 'min')

future_high['minute'] = future_high['ds'].dt.minute
future_high['day_of_week'] = future_high['ds'].dt.dayofweek
future_high = future_high[(future_high['minute']%5==0)]
future_high = future_high[future_high['day_of_week'] < 5]

forecast_high = model.predict(future_high)
fig = model.plot_components(forecast_high)

# COMMAND ----------

# forecast_high

# COMMAND ----------

# DBTITLE 1,low model
ticker = "GC=F"
minutes = 600
df = spark.sql("select * from forexcom_xauusd_3_months")
df = df.filter(f"time > '{one_month_ago}' and time < '{today}'").select("time", "low")
df = df.withColumn("low", F.col("low").cast("double"))\

bars = df.toPandas()
stock = pd.DataFrame(columns = ["time", "low"])
stock = bars
stock = stock.reset_index()
test_size = 100
data_train = stock.iloc[:-test_size, :]
data_test = stock.iloc[-test_size:, :]
df = data_train.reset_index()
df=df.rename(columns={'time':'ds', 'low':'y'})
df['ds'] = pd.to_datetime(df['ds'])
model = Prophet(weekly_seasonality=False)
model.add_seasonality(name='weekly', period=7, fourier_order=10)
model.fit(df)


df_test = data_test.reset_index()
df_test=df_test.rename(columns={'time':'ds', 'low':'y'})
df_test['ds'] = pd.to_datetime(df_test['ds'])

future_low = model.make_future_dataframe(minutes, freq = 'min')

future_low['minute'] = future_low['ds'].dt.minute
future_low['day_of_week'] = future_low['ds'].dt.dayofweek
future_low = future_low[(future_low['minute']%5==0)]
future_low = future_low[future_low['day_of_week'] < 5]

forecast_low = model.predict(future_low)
fig = model.plot_components(forecast_low)

# COMMAND ----------

# Rename 'yhat' columns to their respective labels in each dataframe
forecast_open = forecast_open.rename(columns={'yhat': 'open'})
forecast_high = forecast_high.rename(columns={'yhat': 'high'})
forecast_low = forecast_low.rename(columns={'yhat': 'low'})

# Merge the dataframes on the 'ds' column
merged_forecast = forecast_open[['ds', 'open']].merge(
    forecast_high[['ds', 'high']], on='ds').merge(
    forecast_low[['ds', 'low']], on='ds')

# Display the merged dataframe
merged_forecast.tail()

# COMMAND ----------

ticker = "GC=F"
minutes = 600
df = spark.sql("select * from forexcom_xauusd_3_months")
df = df.filter(f"time > '{one_month_ago}' and time < '{today}'").select("time", "open", "close", "high", "low")
df = df.withColumn("open", F.col("open").cast("double"))\
    .withColumn("close", F.col("close").cast("double"))\
    .withColumn("high", F.col("high").cast("double"))\
    .withColumn("low", F.col("low").cast("double"))
bars = df.toPandas()
stock = pd.DataFrame(columns = ["time", "open", "close", "high", "low"])
stock = bars
stock = stock.reset_index()
test_size = 100
data_train = stock.iloc[:-test_size, :]
data_test = stock.iloc[-test_size:, :]
df = data_train.reset_index()
df=df.rename(columns={'time':'ds', 'close':'y'})
df['ds'] = pd.to_datetime(df['ds'])
model = Prophet(weekly_seasonality=False)
model.add_seasonality(name='weekly', period=7, fourier_order=10)
model.add_regressor('open')
model.add_regressor('high')
model.add_regressor('low')
model.fit(df)


df_test = data_test.reset_index()
df_test=df_test.rename(columns={'time':'ds', 'close':'y'})
df_test['ds'] = pd.to_datetime(df_test['ds'])

future = model.make_future_dataframe(minutes, freq = 'min')
future = future.merge(df_test[['ds', "open", "high", "low"]], on='ds', how='left')
future = future.merge(merged_forecast, on='ds', how='left', suffixes=('', '_merged'))

# Fill missing values (NaN) in 'future' with values from 'merged_forecast'
future['open'] =future['open'].combine_first(future['open_merged'])
future['high'] =future['high'].combine_first(future['high_merged'])
future['low'] = future['low'].combine_first(future['low_merged'])

# Drop the extra merged columns
future = future.drop(columns=['open_merged', 'high_merged', 'low_merged'])

future['minute'] = future['ds'].dt.minute
future['day_of_week'] = future['ds'].dt.dayofweek
future = future[(future['minute']%5==0)]
future = future[future['day_of_week'] < 5]
# future = future.dropna()

forecast = model.predict(future)
fig = model.plot_components(forecast)

# COMMAND ----------

display(df_test)
display(future)

# COMMAND ----------

fig1 = model.plot(forecast)

# COMMAND ----------

# forecast

# COMMAND ----------

# Prepare for accuracy testing
forecast_test = forecast[['ds', 'yhat','yhat_upper','yhat_lower']].tail(test_size) 
forecast_test['y'] = data_test['close'].values 

# Calculate Mean Squared Error (MSE)
mse = mean_squared_error(forecast_test['y'], forecast_test['yhat'])
print(f'Mean Squared Error: {mse}')

# Calculate and print R-squared score
r2 = 1 - (mse / ((forecast_test['y'] - forecast_test['y'].mean()) ** 2).mean())
print(f'R-squared: {r2}')

# Print the forecasted and actual values
print(forecast_test[['ds', 'y', 'yhat']])

# COMMAND ----------

display(forecast_test[['ds', 'yhat', 'yhat_upper','yhat_lower','y']])

# COMMAND ----------

import pytz

# Convert the 'ds' column to datetime (this will retain the timezone information)
forecast_test['ds'] = pd.to_datetime(forecast_test['ds'])
forecast_test['ds'] = forecast_test['ds'].dt.tz_localize('UTC')
# Define the Chicago timezone
chicago_tz = pytz.timezone('America/Chicago')

# Apply timezone conversion from UTC to Chicago
forecast_test['ds'] = forecast_test['ds'].apply(lambda x: x.astimezone(chicago_tz))


# COMMAND ----------

pd.set_option('display.max_rows', 10000)

# COMMAND ----------

print(forecast_test)

# COMMAND ----------

# Get the current time in Chicago timezone, set to the start of the current hour
current_hour = datetime.now(chicago_tz).replace(minute=0, second=0, microsecond=0)

# Get the next hour by adding one hour to the current hour
next_hour = current_hour + timedelta(hours=1)

# Convert 'ds' to Chicago timezone and filter the data for the current hour to the next hour
forecast_test['ds'] = pd.to_datetime(forecast_test['ds']).apply(lambda x: x.astimezone(chicago_tz))
filtered_data = forecast_test[(forecast_test['ds'] >= current_hour) & (forecast_test['ds'] < next_hour)]

# Convert the filtered pandas DataFrame to a Spark DataFrame
spark_df = spark.createDataFrame(filtered_data)

# Write the filtered data to a table in Databricks
table_name = "month_model_daily_forecast"
spark_df.write.mode("append").saveAsTable(table_name)

# COMMAND ----------

