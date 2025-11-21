# Cafe Sales Daily ETL
This project simulates the role of a data engineer in designing and setting up a daily ETL pipeline for a cafe, orchestrated with Apache Airflow (Continue from `1-Cafe-Orders-Setup` project). 

The daily orders dataset `daily_cafe_sales.csv` is located in `data_mocking_data_API` folder to demonstate API data extraction.

Three core ETL tasks are defined in the DAG file `Cafe-Orders-Daily-ETL.py` (located in the `dags/ `folder):

1. Extract : Use `requests` to simulate data extraction via API and save it to a local file.

2. Transform : Reuse the same transformation logic from the `1-Cafe-Orders-Setup` project. For readability, transformation functions are placed in `myETLtools.py`. (Note : `myETLtools.py` must also be inside the `dags/` folder so Airflow can import it)

3. Load : Load the daily fact table into a local PostgreSQL database.

## Requirements
1. [Apache Airflow (using 3.1.0)](https://airflow.apache.org/docs/apache-airflow/3.1.3/start.html)
2. [Pyspark (using 4.0.1)](https://spark.apache.org/docs/latest/api/python/index.html)
3. [apache-airflow-providers-apache-spark (using 5.3.3)](https://airflow.apache.org/docs/apache-airflow-providers-apache-spark/stable/index.html)

Note : All files inside the `dags/` folder should be copied into your `<airflow_home>/dags/` directory

## Contact
Settawuth Rattanaudom (Zoom) | settawuth.r@gmail.com