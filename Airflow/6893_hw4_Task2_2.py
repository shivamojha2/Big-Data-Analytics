#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import json
import yfinance as yf
import numpy as np
import pandas as pd
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from sklearn.linear_model import LinearRegression

default_args = {
    'owner': 'Shivam',
    'depends_on_past': False,
    'email': ['123@columbia.edu'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(seconds=30)
}

data_dir = '/home/airflow/dags/task2_2/'
json_file = 'errors_dict.json'
date = datetime.now().strftime("%Y-%m-%d")
#strt_date = datetime.strptime(date, "%Y-%m-%d") - timedelta(weeks=27)

def fetch_data(ticker):
  input_company_data = yf.Ticker(ticker)
  # Fetching data for the last 6 months
  # df = input_company_data.history(start=strt_date, end=date)
  df = input_company_data.history(period = '6mo')
  df.to_csv(data_dir + ticker + '.csv')

def preprocess_data(ticker):
  df = pd.read_csv(data_dir + ticker + '.csv')
  df['Date'] = pd.to_datetime(df['Date'])
  # The data doesn't necessarily need to be normalised
  # Cleaning the data by dropping null values and any date values greater than today
  df = df[df.Date <= pd.to_datetime(date)].reset_index(drop = True)
  df = df.dropna()
  df.to_csv(data_dir + ticker + '.csv')

def init_df_dict():
  errors_dict = {'Date': '', 'AAPL': '', 'GOOGL': '', 'FB': '', 'MSFT': '', 'AMZN': ''}
  errors_dict['Date'] = [date]

  with open(data_dir + json_file, 'w') as jsonfile: 
    json.dump(errors_dict, jsonfile)

def predict_and_compute_error(ticker):
  df = pd.read_csv(data_dir + ticker + '.csv')
  df = df.iloc[:-1, :]

  # Generate training data
  X = np.array(df[['Open', 'High', 'Low', 'Close', 'Volume']])
  y = np.array(df['High'])
  
  # When we fetch data till 30th November morning, we get stock values till market close on 29th. 
  # So using data till 28th November to train. Pushing the labels value ahead by 1 index as well.
  # error = (prediction yesterday - actual price today)/actual price today
  # example: error = (prediction on 28th for 29th - price on 29th)/ price on 29th
  X_train = X[:-2, :]
  y_train = y[1:-1]

  # Run Linear Regression Model
  lr_model = LinearRegression().fit(X_train, y_train)
  print("Score: " + str(lr_model.score(X_train, y_train)))

  # Make predictions
  X_test = X[-2:-1, :] # Last value in dataframe
  y_test = y[-1] # Last High Value

  preds = lr_model.predict(np.array(X_test))
  preds = preds[0]
    
  # Read error json
  with open(data_dir + json_file) as j:
    errors = json.load(j)
  
  # Compute error
  if not(np.array_equal(X[-2:-1, :], X[-3:-2, :], equal_nan=False)):
      er = (preds - y_test) / y_test
      errors[ticker] = [er]

      # Update error json
      with open(data_dir + json_file, 'w') as j:
        json.dump(errors, j)

def generate_output_csv():
  # Read error json
  with open(data_dir + json_file) as j:
    errors = json.load(j)

  df = pd.DataFrame(errors)
  df.to_csv(data_dir + 'csv_files/' + 'results_' + date + '.csv')

  csv_list = []
  for file in os.listdir(data_dir+'csv_files/'):
    if (file.startswith('results_') and file.endswith('.csv')):
      csv_list.append(os.path.join(data_dir + 'csv_files/', file))
  
  # Compute compiled csv output
  out = pd.DataFrame(columns=['Date','AAPL','GOOGL','FB', 'MSFT','AMZN'])
  for item in csv_list:
      df1 = pd.read_csv(item, index_col=[0])
      out = out.append(df1)
  out.sort_values(by=['Date'], inplace=True, ascending=True)
  out.to_csv(data_dir + 'compiled_error_results.csv')

with DAG(
    'Big_Data_HW4_Task2_2',
    default_args = default_args,
    description = 'DAG for Q2.2 HW4',
    start_date = datetime(2021, 11, 30, 7, 0, 0),
    schedule_interval = '* 7 * * *',
    catchup = False,
    tags = ['example'],
) as dag:

  t1 = PythonOperator(
    task_id = 't1',
    python_callable = init_df_dict,
  )

  t2 = PythonOperator(
    task_id = 't2',
    python_callable = fetch_data,
    op_kwargs = {'ticker': 'AAPL'}
  )

  t3 = PythonOperator(
    task_id = 't3',
    python_callable = preprocess_data,
    op_kwargs = {'ticker': 'AAPL'}
  )

  t4 = PythonOperator(
    task_id = 't4',
    python_callable = predict_and_compute_error,
    op_kwargs = {'ticker': 'AAPL'}
  )

  t5 = PythonOperator(
    task_id = 't5',
    python_callable = fetch_data,
    op_kwargs = {'ticker': 'GOOGL'}
  )

  t6 = PythonOperator(
    task_id = 't6',
    python_callable = preprocess_data,
    op_kwargs = {'ticker': 'GOOGL'}
  )

  t7 = PythonOperator(
    task_id = 't7',
    python_callable = predict_and_compute_error,
    op_kwargs = {'ticker': 'GOOGL'}
  )

  t8 = PythonOperator(
    task_id = 't8',
    python_callable = fetch_data,
    op_kwargs = {'ticker': 'FB'}
  )

  t9 = PythonOperator(
    task_id = 't9',
    python_callable = preprocess_data,
    op_kwargs = {'ticker': 'FB'}
  )

  t10 = PythonOperator(
    task_id = 't10',
    python_callable = predict_and_compute_error,
    op_kwargs = {'ticker': 'FB'}
  )


  t11 = PythonOperator(
    task_id = 't11',
    python_callable = fetch_data,
    op_kwargs = {'ticker': 'MSFT'}
  )

  t12 = PythonOperator(
    task_id = 't12',
    python_callable = preprocess_data,
    op_kwargs = {'ticker': 'MSFT'}
  )

  t13 = PythonOperator(
    task_id = 't13',
    python_callable = predict_and_compute_error,
    op_kwargs = {'ticker': 'MSFT'}
  )


  t14 = PythonOperator(
    task_id = 't14',
    python_callable = fetch_data,
    op_kwargs = {'ticker': 'AMZN'}
  )

  t15 = PythonOperator(
    task_id = 't15',
    python_callable = preprocess_data,
    op_kwargs = {'ticker': 'AMZN'}
  )

  t16 = PythonOperator(
    task_id = 't16',
    python_callable = predict_and_compute_error,
    op_kwargs = {'ticker': 'AMZN'}
  )

  t17 = PythonOperator(
    task_id = 't17',
    python_callable = generate_output_csv,
  )

  # Task dependencies
  t1 >> [t2, t5, t8, t11, t14]
  t2 >> t3
  t3 >> t4
  t5 >> t6
  t6 >> t7
  t8 >> t9
  t9 >> t10
  t11 >> t12
  t12 >> t13
  t14 >> t15
  t15 >> t16
  [t4, t7, t10, t13, t16] >> t17
