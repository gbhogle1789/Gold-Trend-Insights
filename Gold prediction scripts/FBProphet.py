# Databricks notebook source
!pip install prophet
!pip install yfinance

# COMMAND ----------

import pandas as pd
import numpy as np
from datetime import datetime
from prophet import Prophet
import matplotlib.pyplot as plt
from sklearn.metrics import mean_absolute_error, mean_squared_error
import warnings
import pickle
import json
import os
import yfinance as yf
import pyspark.sql.functions as F



# COMMAND ----------

# ticker = "GC=F"
# minutes = 300
# df = spark.sql("select * from forexcom_xauusd_3_months")
# df = df.filter("time > '2024-10-01' and time < '2024-10-19'").select("time", "open", "close", "high", "low")
# df = df.withColumn("open", F.col("open").cast("double"))\
#     .withColumn("close", F.col("close").cast("double"))\
#     .withColumn("high", F.col("high").cast("double"))\
#     .withColumn("low", F.col("low").cast("double"))
# bars = df.toPandas()
# stock = pd.DataFrame(columns = ["time", "open", "close", "high", "low"])
# stock = bars
# stock = stock.reset_index()
# test_size = 50
# data_train = stock.iloc[:-test_size, :]
# data_test = stock.iloc[-test_size:, :]
# df = data_train.reset_index()
# df=df.rename(columns={'time':'ds', 'close':'y'})
# df['ds'] = pd.to_datetime(df['ds'])
# model = Prophet(daily_seasonality=True)
# model.add_regressor('open')
# model.add_regressor('high')
# model.add_regressor('low')
# model.fit(df)


# df_test = data_test.reset_index()
# df_test=df_test.rename(columns={'time':'ds', 'close':'y'})
# df_test['ds'] = pd.to_datetime(df_test['ds'])

# future = model.make_future_dataframe(minutes, freq = 'min')
# future = future.merge(df_test[['ds', "open", "high", "low"]], on='ds', how='left')

# future['minute'] = future['ds'].dt.minute
# future = future[(future['minute']%5==0)]

# # future = future.fillna(ffill=True)

# forecast = model.predict(future)
# fig = model.plot_components(forecast)

# COMMAND ----------

ticker = "GC=F"
minutes = 560
df = spark.sql("select * from forexcom_xauusd_3_months")
df = df.filter("time > '2024-09-01' and time < '2024-10-18'").select("time", "open", "close", "high", "low")
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
model = Prophet(daily_seasonality=True)
model.add_regressor('open')
model.add_regressor('high')
model.add_regressor('low')
model.fit(df)


df_test = data_test.reset_index()
df_test=df_test.rename(columns={'time':'ds', 'close':'y'})
df_test['ds'] = pd.to_datetime(df_test['ds'])

future = model.make_future_dataframe(minutes, freq = 'min')
future = future.merge(df_test[['ds', "open", "high", "low"]], on='ds', how='left')

future['minute'] = future['ds'].dt.minute
future = future[(future['minute']%5==0)]

future = future.dropna()

forecast = model.predict(future)
fig = model.plot_components(forecast)

# COMMAND ----------

fig1 = model.plot(forecast)

# COMMAND ----------

display(future)
display(df_test)

# COMMAND ----------

# Prepare for accuracy testing
forecast_test = forecast[['ds', 'yhat']].tail(test_size) 
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

