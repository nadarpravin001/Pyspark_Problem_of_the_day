# Databricks notebook source
# DBTITLE 1,Import required PySpark modules
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window   

# COMMAND ----------

# DBTITLE 1,Loading data into dataframe
data = [('A','Bangalore','A@gmail.com',1,'CPU'),
('A','Bangalore','A1@gmail.com',1,'CPU'),
('A','Bangalore','A2@gmail.com',2,'DESKTOP'),
('B','Bangalore','B@gmail.com',2,'DESKTOP'),
('B','Bangalore','B1@gmail.com',2,'DESKTOP'),
('B','Bangalore','B2@gmail.com',1,'MONITOR')]

schema = ['name' ,'address' ,'email','floor','resources']

df = spark.createDataFrame(data = data, schema = schema)

# COMMAND ----------

# DBTITLE 1,SOLUTION
# Calculate Total visits and resources used.
df_total_vst_res_used = (
    df.groupBy("name")
      .agg(
          count("floor").alias("total_visits")
          ,concat_ws(',',collect_set("resources")).alias("resources_used")
      )
)

# Calculate most visited floor
df_mst_vsted = (
    df.groupBy("name","floor")
      .agg(
          count("*").alias("floor_visits")
      ).withColumn(
          "rnk", rank().over(Window.partitionBy("name").orderBy(col("floor_visits").desc()))
      ).filter(
          col("rnk") == 1
      ).select(
          "name",col("floor").alias("most_visited_floor")
      )
)

# Comine 
final_df = (
    df_total_vst_res_used.join(df_mst_vsted, "name", "inner")
        .select("name", "total_visits", "most_visited_floor", "resources_used")
)