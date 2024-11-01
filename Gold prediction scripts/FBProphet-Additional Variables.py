# Databricks notebook source
!pip install prophet
!pip install yfinance==0.2.44 

# COMMAND ----------

# MAGIC %restart_python

# COMMAND ----------

import pandas as pd
import numpy as np
from datetime import datetime
from prophet import Prophet
import matplotlib.pyplot as plt
import warnings
from sklearn.metrics import mean_absolute_error, mean_squared_error
import json
import os
import yfinance as yf

# COMMAND ----------

ticker = "GC=F"
days = 30
stock = pd.DataFrame(columns = ['Date','Open','High','Low', 'Close','Adj Close','Volume'])
end = datetime.now()
start = datetime(end.year - 24,end.month,end.day)
# bars = pdr.get_data_yahoo(ticker,start,end)['Close']
bars = yf.download(ticker,start,end)
stock = bars
stock = stock.reset_index()
test_size = 30
data_train = stock.iloc[:-test_size, :]
data_test = stock.iloc[-test_size:, :]
df = data_train.reset_index()
df=df.rename(columns={'Date':'ds', 'Close':'y'})
df['ds'] = pd.to_datetime(df['ds'])
model = Prophet(daily_seasonality=True)
model.add_regressor('Volume')
model.fit(df)

future = model.make_future_dataframe(days, freq = 'D')
future = future.merge(df[['ds', 'Volume']], on='ds', how='left')
future['Volume'].fillna(0, inplace=True)
future['day'] = future['ds'].dt.weekday
future = future[(future['day']<5)]
forecast = model.predict(future)
fig = model.plot_components(forecast)

# COMMAND ----------

fig1 = model.plot(forecast)

# COMMAND ----------

# Prepare for accuracy testing
forecast_test = forecast[['ds', 'yhat']].tail(test_size) 
forecast_test['y'] = data_test['Close'].values 

# Calculate Mean Squared Error (MSE)
mse = mean_squared_error(forecast_test['y'], forecast_test['yhat'])
print(f'Mean Squared Error: {mse}')

# Calculate and print R-squared score
r2 = 1 - (mse / ((forecast_test['y'] - forecast_test['y'].mean()) ** 2).mean())
print(f'R-squared: {r2}')

# Print the forecasted and actual values
print(forecast_test[['ds', 'y', 'yhat']])

# COMMAND ----------

