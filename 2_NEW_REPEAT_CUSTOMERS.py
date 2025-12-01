# Databricks notebook source
# DBTITLE 1,Import required PySpark modules
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window   

# COMMAND ----------

# DBTITLE 1,Loading data into dataframe
data = [(1,100,'2022-01-01',2000)
,(2,200,'2022-01-01',2500)
,(3,300,'2022-01-01',2100)
,(4,100,'2022-01-02',2000)
,(5,400,'2022-01-02',2200)
,(6,500,'2022-01-02',2700)
,(7,100,'2022-01-03',3000)
,(8,400,'2022-01-03',1000)
,(9,600,'2022-01-03',3000)]

schema = ['order_id' ,'customer_id' ,'order_date','order_amount']

df = spark.createDataFrame(data = data, schema = schema)

df = (
    df.withColumn("order_date", col("order_date").cast("date"))
)


# COMMAND ----------

# DBTITLE 1,SOLUTION
# first order date for each customer.
df_first_time_data = (
    df.groupby("customer_id")
      .agg(min("order_date").alias("First_time"))
) 

# Calculating New Repeat Customers.
final_df = (
    df.alias("df").join(
        df_first_time_data.alias("df_first_time_data"), col("df.customer_id") == col("df_first_time_data.customer_id"), "inner"
    ).groupBy(col("df.order_date"))
     .agg(
         sum(when(col("df.order_date") == col("df_first_time_data.First_time"), 1).otherwise(0)).alias("New_customer")
         ,sum(when(col("df.order_date") != col("df_first_time_data.First_time"), 1).otherwise(0)).alias("Repeat_customer")
         ,sum(when(col("df.order_date") == col("df_first_time_data.First_time"), col("df.order_amount")).otherwise(0)).alias("New_customer_amount")
         ,sum(when(col("df.order_date") != col("df_first_time_data.First_time"), col("df.order_amount")).otherwise(0)).alias("Repeat_customer_amount")
     )
)