from sqlalchemy import create_engine
import pandas as pd
import random
from pyspark.sql import SparkSession, types as t, functions as F, Window

def data_gen(df):
    # df.dropna() contain ~3000 rows, assuming 300-900 daily orders
    n = random.uniform(0.1,0.3)
    daily_fact = df.sample(n)

    daily_df = (daily_df
    .withColumn("Transaction ID",F.concat(F.lit("TXN_"),F.expr("substr(cast(rand() as string), 3, 7)")))
    .withColumn("Transaction Date", F.expr("cast(current_date() as string)"))
    )
    return daily_fact

def transform(df,spark):
    # note: only use Spark SQL (A Spark module for structured data processing)
    df = df.replace(['ERROR', 'UNKNOWN','None'], [None,None,None])
    df = df.toDF('transaction_id',
    'item_name',
    'item_quantity',
    'item_ppu', # price per unit
    'item_total_price',
    'payment_type',
    'location_type',
    'transaction_date'
    )
    df = (df.withColumn("item_quantity", F.col("item_quantity").cast("integer")) 
    .withColumn("item_ppu", F.col("item_ppu").cast("double")) 
    .withColumn("item_total_price", F.col("item_total_price").cast("double"))
    .withColumn("transaction_date", F.col("transaction_date").cast("date"))
    )
    df = df.withColumn("item_ppu", 
    F.when(F.col("item_ppu").isNull(),
    F.when(F.col("item_name") == 'Cookie', 1)
    .when(F.col("item_name") == 'Tea', 1.5)
    .when(F.col("item_name") == 'Coffee', 2)
    .when(F.col("item_name") == 'Juice', 3)
    .when(F.col("item_name") == 'Cake', 3)
    .when(F.col("item_name") == 'Sandwich', 4)
    .when(F.col("item_name") == 'Smoothie', 4)
    .when(F.col("item_name") == 'Salad', 5)
    )
    .otherwise(F.col("item_ppu"))
    )
    df = df.withColumn('item_total_price',
    F.when(F.col("item_total_price").isNull(),
    df.item_quantity * df.item_ppu)
    .otherwise(df.item_total_price)
    )
    df = df.withColumn('item_ppu',
    F.when(F.col("item_ppu").isNull(),
    df.item_total_price / df.item_quantity)
    .otherwise(df.item_ppu)
    )
    df = df.withColumn('item_quantity',
    F.when(F.col("item_quantity").isNull(),
    df.item_total_price / df.item_ppu)
    .otherwise(df.item_quantity)
    )
    df = df.withColumn("item_name", 
    F.when(F.col("item_name").isNull(),
    F.when(F.col("item_ppu") == 1.0, 'Cookie')
    .when(F.col("item_ppu") == 1.5, 'Tea')
    .when(F.col("item_ppu") == 2.0, 'Coffee')
    .when(F.col("item_ppu") == 5.0, 'Salad')
    )
    .otherwise(F.col("item_name"))
    )

    # create postgreSQL engine
    engine = create_engine("postgresql://airflow:airflow@localhost:5432/airflow")

    dim_item = spark.createDataFrame(pd.read_sql("SELECT * FROM dim_item", engine))
    dim_payment = spark.createDataFrame(pd.read_sql("SELECT * FROM dim_payment", engine))
    dim_location = spark.createDataFrame(pd.read_sql("SELECT * FROM dim_location", engine))

    df = df.join(F.broadcast(dim_item), on="item_name", how="left")
    df = df.join(F.broadcast(dim_payment), on="payment_type", how="left")
    df = df.join(F.broadcast(dim_location), on="location_type", how="left")

    df = (df.fillna(0)
        .withColumn("item_quantity", F.col("item_quantity").cast("integer")) 
        .withColumn("item_id", F.col("item_id").cast("integer")) 
        .withColumn("item_total_price", F.col("item_total_price").cast("double"))
        .withColumn("transaction_date", F.col("transaction_date").cast("date"))
        )
    fact_order = df.select(
    "transaction_id",
    "item_id",
    "item_quantity",
    "item_total_price",
    "payment_type_id",
    "location_type_id",
    "transaction_date"
    )
    return fact_order