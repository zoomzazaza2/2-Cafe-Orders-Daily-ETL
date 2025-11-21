from airflow.sdk import dag, task
# from airflow.providers.common.bash.operators.sql import BashOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from sqlalchemy import create_engine

import pendulum
import datetime
import requests

import kagglehub

import os
import pandas as pd
import random
from datetime import date
from pyspark.sql import SparkSession, functions as F
from myETLtools import transform

@dag(
    dag_id="Cafe_Orders_Daily_ETL",
    schedule='@daily',
    start_date=pendulum.datetime(2025, 11, 1, tz="local"),
)

def cafe_orders_daily_etl():

    @task
    def af_extract() -> str:
        url = '''https://raw.githubusercontent.com/zoomzazaza2/2-Cafe-Orders-Daily-ETL/refs/heads/main/data_mocking_data_API/daily_cafe_sales.csv'''
        response = requests.request("GET", url)

        data_path = "/home/setta/projects_venv/2-Cafe-Orders-Daily-ETL/data/daily_cafe_sales.csv"
        os.makedirs(os.path.dirname(data_path), exist_ok=True)

        with open(data_path, "w") as file:
            file.write(response.text)
        return data_path

    @task.pyspark()
    def af_transfrom(data_path: str, spark: SparkSession) -> pd.DataFrame:
        df = spark.read.csv(data_path,header=True)
        daily_fact_order = transform(df,spark)
        return daily_fact_order.toPandas()

    @task
    def af_load(df: pd.DataFrame):
        engine = create_engine("postgresql://airflow:airflow@localhost:5432/airflow")
        df.to_sql("fact_order",
        engine,
        if_exists="append",  
        index=False
        )

    path = af_extract()
    df = af_transfrom(path)
    l1 = af_load(df)

    path >> df >> l1

dag = cafe_orders_daily_etl()  
