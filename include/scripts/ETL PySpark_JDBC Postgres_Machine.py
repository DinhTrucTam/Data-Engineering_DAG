from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, mean, stddev, lit, round, to_timestamp, year, month, dayofmonth, hour, minute
from pyspark.sql.types import TimestampType
import sys

file_path = "./include/machine.csv"

def spark_session_initialization():
    # Initialize Spark session
    spark = SparkSession.builder \
        .appName("Spark_Postgres") \
        .config("spark.jars", "/usr/local/airflow/include/postgresql-42.7.2.jar") \
        .getOrCreate()
    
    print("Spark Session is initialized successfully")

    return spark

def data_ingestion_machine(spark):
    # Load dataset
    df_2 = spark.read.csv(file_path, header=True, inferSchema=True)
    # Check if DataFrame is empty
    if df_2.isEmpty():
        print("Dataset is empty. Exiting...")
        sys.exit()
    else: print("Data Ingestion (Machine) is completed")

    return df_2

def data_load(df):
    """Write a DataFrame to a PostgreSQL database table"""
    pg_url = "jdbc:postgresql://postgres:5432/DA_DW"

    pg_properties = {
        "user": "postgres",
        "password": "postgres",
        "driver": "org.postgresql.Driver"
    }

    #Mapping the DataFrame to PostgreSQL Table
    # Loading Machine
    df_machine = df.select(
        col("Machine_Id").alias("machine_id"),
        col("Machine_Name").alias("machine_name"),
        col("Machine_Type").alias("machine_type"),
        col("Stage").alias("stage"),
        to_timestamp(col("Last_Maintenance_Date"), "yyyy-MM-dd'T'HH:mm:ss.SSSXXX").alias("last_maintenance_date")
    )
    df_machine.write.jdbc(pg_url, "Dim_Machine", mode="append", properties=pg_properties)

if __name__ == "__main__":
    spark = spark_session_initialization()
    df = data_ingestion_machine(spark)
    data_load(df)