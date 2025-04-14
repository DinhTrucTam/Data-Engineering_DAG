from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, mean, stddev, lit, round, to_timestamp, year, month, dayofmonth, hour, minute
from pyspark.sql.types import TimestampType
import sys



def main():
    # spark = SparkSession.builder \
    #     .appName("PySpark Example") \
    #     .getOrCreate()
    
    # df = spark.read.csv("./include/data.csv", header="true")
    # df.show()
    
    # spark.stop()
    # Redirect stdout and stderr to a log file

    # Initialize Spark session with increased memory
    spark = SparkSession.builder \
        .appName("DataProfiling") \
        .config("spark.driver.memory", "2g") \
        .config("spark.executor.memory", "2g") \
        .config("spark.jars", "/usr/local/airflow/include/postgresql-42.7.2.jar") \
        .getOrCreate()
    
    # PostgreSQL connection parameters
    pg_url = "jdbc:postgresql://127.0.0.1:5432/DA_DW"  # Change localhost and database name
    pg_properties = {
        "user": "postgres",        # Replace with your username
        "password": "postgres",  # Replace with your password
        "driver": "org.postgresql.Driver"
    }
    # Adjust Spark settings
    spark.conf.set("spark.sql.debug.maxToStringFields", "100")

    print("Data Loading...")

    # Load dataset
    file_path = "./include/source_data_manufacturing_2025-01-04.csv"
    df = spark.read.csv(file_path, header=True, inferSchema=True)

    # Check if DataFrame is empty
    if df.isEmpty():
        print("Dataset is empty. Exiting...")
        sys.exit()

    # Replace dots in column names with underscores
    df = df.toDF(*[c.replace(".", "_") for c in df.columns])

    # Convert time_stamp column to TimestampType
    df = df.withColumn("time_stamp", col("time_stamp").cast(TimestampType()))

    # Handle missing values
    numeric_columns = [c for c, t in df.dtypes if t in ('int', 'double')]

    # Replace negative values with NULL
    for column in numeric_columns:
        df = df.withColumn(column, when(col(column) < 0, None).otherwise(col(column)))

    # Handling Outliers using Z-score method
    for column in numeric_columns:
        stats = df.select(mean(col(column)).alias("mean"), stddev(col(column)).alias("stddev")).collect()
        mean_val = stats[0]["mean"]
        stddev_val = stats[0]["stddev"]

        if stddev_val is not None and stddev_val > 0.01:  # Adjust threshold
            df = df.withColumn(column, when((col(column) - lit(mean_val)) / lit(stddev_val) > 3, lit(None)).otherwise(col(column)))


    # Compute mean values for numeric columns
    mean_values = {col_name: df.select(mean(col(col_name))).collect()[0][0] for col_name in numeric_columns}
    mean_values = {k: v for k, v in mean_values.items() if v is not None}

    # Apply fillna only if there are valid mean values
    if mean_values:
        df = df.fillna(mean_values)

    # Round real number values to 2 decimal places
    for column in numeric_columns:
        df = df.withColumn(column, round(col(column), 2))

    # Force execution to avoid lazy evaluation issues
    df.cache()
    df.count()
    df.show()

    print("Process Completed Successfully.")

if __name__ == "__main__":
    main()