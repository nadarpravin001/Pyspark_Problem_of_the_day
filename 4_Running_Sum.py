# Databricks notebook source
# DBTITLE 1,Import required PySpark modules
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window   

# COMMAND ----------

# DBTITLE 1,Function to load data into a dataframe
def read_from_file(file_format, file_location, delimiter = ','):
    try:
        file_df = (
            spark.read.format(file_format)
                      .option("header", "true")
                      .option("inferSchema", "true")
                      .option("delimiter", delimiter)
                      .load(file_location)
        )
        return file_df
    except Exception as e:
        error = f"{e}"
        print("Error: "+ error)

# COMMAND ----------

# DBTITLE 1,Loading data into dataframe
delimitter = "csv"
source_location = "/FileStore/tables/sample_data/sample_customer_data.csv"
df = read_from_file(delimitter, source_location)

df = (
    df.withColumn("year", year(col("Order_Date")))
)

# COMMAND ----------

# DBTITLE 1,SOLUTION
# Calculate Year Month Sales
df_year_month_sales = (
    df.groupBy("year","month")
      .agg(
          round(sum("Sales"),2).alias("year_month_sales")
      )
)

# Rolling months calulation
df_rolling_months_cal = (
    df_year_month_sales
    .withColumn(
        "Running_sum_previous", sum(col("year_month_sales")).over(Window.orderBy(col("year").asc(),col("month").asc()).rowsBetween(-1,Window.currentRow))
    ).withColumn(
        "Running_sum_next", sum(col("year_month_sales")).over(Window.orderBy(col("year").asc(),col("month").asc()).rowsBetween(Window.currentRow,1))
    ).withColumn(
        "Running_sum_avg", sum(col("year_month_sales")).over(Window.orderBy(col("year").asc(),col("month").asc()).rowsBetween(-1,1))
    ).withColumn(
        "Running_sum_min", min(col("year_month_sales")).over(Window.orderBy(col("year").asc(),col("month").asc()).rowsBetween(-1,1))
    )
)