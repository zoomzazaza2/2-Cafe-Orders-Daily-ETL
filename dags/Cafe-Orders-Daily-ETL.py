from airflow.sdk import dag, task
from airflow.providers.common.bash.operators.sql import BashOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

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
    # catchup=False,
    # dagrun_timeout=datetime.timedelta(minutes=60),
)

def cafe_orders_daily_etl():

    # @task.pyspark()
    # def af_data_gen(spark: SparkSession) -> str:
    #     raw_data_path = kagglehub.dataset_download("ahmedmohamed2003/cafe-sales-dirty-data-for-cleaning-training");
    #     df = spark.read.csv(raw_data_path+'/dirty_cafe_sales.csv',header=True);
    #     n = random.uniform(0.1,0.3)
    #     daily_df = df.sample(n)
    #     daily_df = (daily_df
    #     .withColumn("Transaction ID",F.concat(F.lit("TXN_"),F.expr("substr(cast(rand() as string), 3, 7)")))
    #     .withColumn("Transaction Date", F.expr("cast(current_date() as string)"))
    #     )
    #     data_path = "/home/setta/projects_venv/2-Cafe-Orders-Daily-ETL/data/"
    #     filename = "daily_cafe_sales.csv"
    #     daily_df.toPandas().to_csv(data_path+filename,index=False,header=True)
    #     return data_path

    # aF_daily_upload = BashOperator(
    #     task_id="aF_daily_upload",
    #     bash_command='''
    #     cd /home/setta/projects_venv/2-Cafe-Orders-Daily-ETL/data/
    #     git add daily_cafe_sales.csv
    #     git commit -m "Add daily cafe sales data"
    #     git push origin main
    #     '''
    # ) 

    @task
    def af_extract() -> str:
        url = '''
        https://raw.githubusercontent.com/zoomzazaza2/2-Cafe-Orders-Daily-ETL/refs/heads/main/data(mocking_data_API)/daily_cafe_sales.csv
        '''
        response = requests.request("GET", url)

        data_path = "/home/setta/projects_venv/2-Cafe-Orders-Daily-ETL/data(staging)/daily_cafe_sales.csv"
        os.makedirs(os.path.dirname(data_path), exist_ok=True)

        with open(data_path, "w") as file:
            file.write(response.text)
        return data_path

    @task.pyspark()
    def af_transfrom(data_path: str, spark: SparkSession) -> pd.DataFrame:
        df = spark.read.csv(data_path,header=True)
        daily_fact_order = transform(df)
        return daily_fact_order.toPandas()

    @task
    def af_load1(df: pd.DataFrame):
        # define destination path
        clean_path = "/home/setta/clean_data/clean_cafe_sales.csv"
        # change data types
        df.item_id = df.item_id.astype('Int64')
        df.item_quantity = df.item_quantity.astype('Int64')
        df.payment_type_id = df.payment_type_id.astype('Int64')
        df.location_type_id = df.location_type_id.astype('Int64')
        # load to csv
        df.to_csv(clean_path,index=False,header=True)


        # ต้องอัพโหลด dimension table ก่อน





        # load to database
        postgres_hook = PostgresHook(postgres_conn_id="pg_default")
        conn = postgres_hook.get_conn()
        cur = conn.cursor()
        # create empty temp table

        cur.execute("TRUNCATE TABLE fact_orders_temp;")
        # copy to empty temp table
        with open(clean_path, "r") as file:
            cur.copy_expert(
                "COPY fact_orders_temp FROM STDIN WITH CSV HEADER DELIMITER ',' QUOTE '\"'",
                file,
            )
        conn.commit()
        cur.close()
        conn.close()
 

    af_load2 = SQLExecuteQueryOperator(
        task_id="af_load2",
        conn_id="pg_default",
        sql="load_db.sql"
    ) 

    path = af_extract()
    df = af_transfrom(path)
    l1 = af_load1(df)

    path >> df >> l1 >> af_load2

dag = cafe_orders_daily_etl()  
