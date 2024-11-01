# Databricks notebook source
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd

# COMMAND ----------

dataset = pd.read_csv("/Volumes/workspace/default/gold_data/FOREXCOM_XAUUSD 3 Months - Copy.csv")

# COMMAND ----------

dataset[['time', 'open', 'high', 'low', 'close']]

# COMMAND ----------

# Floor Pivot Points (Basic Pivot Points) - Support and Resistance
# https://www.investopedia.com/trading/using-pivot-points-for-predictions/
PP = pd.Series((dataset['high'] + dataset['low'] + dataset['close']) / 3)  
R1 = pd.Series(2 * PP - dataset['low'])  
S1 = pd.Series(2 * PP - dataset['high'])  
R2 = pd.Series(PP + dataset['high'] - dataset['low'])  
S2 = pd.Series(PP - dataset['high'] + dataset['low'])  
R3 = pd.Series(dataset['high'] + 2 * (PP - dataset['low']))  
S3 = pd.Series(dataset['low'] - 2 * (dataset['high'] - PP))
R4 = pd.Series(dataset['high'] + 3 * (PP - dataset['low']))  
S4 = pd.Series(dataset['low'] - 3 * (dataset['high'] - PP))
R5 = pd.Series(dataset['high'] + 4 * (PP - dataset['low']))  
S5 = pd.Series(dataset['low'] - 4 * (dataset['high'] - PP))
P = pd.Series((dataset['open'] + (dataset['high'] + dataset['low'] + dataset['close'])) / 4) # Opening Price Formula
psr = {'P':P, 'R1':R1, 'S1':S1, 'R2':R2, 'S2':S2, 'R3':R3, 'S3':S3,'R4':R4, 'S4':S4,'R5':R5, 'S5':S5}  
PSR = pd.DataFrame(psr)  
dataset = dataset.join(PSR)
print(dataset.head())

# COMMAND ----------

pivot_point = pd.concat([dataset['close'],P,R1,S1,R2,S2,R3,S3],axis=1).plot(figsize=(22,22),grid=True)
plt.title('Gold Pivot Point')
plt.legend(['Price','P','R1','S1','R2','S2','R3','S3'], loc=0)
plt.show()