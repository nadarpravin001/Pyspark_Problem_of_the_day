# Databricks notebook source
# DBTITLE 1,Import required PySpark modules
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window

# COMMAND ----------

# DBTITLE 1,Loading data into dataframe
data = [('India','SL','India'),
('SL','Aus','Aus'),
('SA','Eng','Eng'),
('Eng','NZ','NZ'),
('Aus','India','India')]

schema = ['Team_1' ,'Team_2' ,'winner']

df = spark.createDataFrame(data = data, schema = schema)

# COMMAND ----------

# DBTITLE 1,SOLUTION
# assign 1 point if Team_1 is winner, else 0
df_1 = (
    df.withColumn("point", when((col("Team_1") == col("winner")), 1).otherwise(0))
      .select(col("Team_1").alias("Team_name"), "point")
)

# assign 1 point if Team_2 is winner, else 0
df_2 = (
    df.withColumn("point", when((col("Team_2") == col("winner")), 1).otherwise(0))
      .select(col("Team_2").alias("Team_name"), "point")
)

# Combine points from Team_1 and Team_2 results
df1_df_2_union = (
    df_1.union(df_2)
)

# Final aggregation: matches played, wins, losses for each team
final_df = (
    df1_df_2_union.groupBy("Team_name")
                  .agg(
                        count("point").alias("matches_played")
                        ,sum("point").alias("No_of_wins")
                        ,(count("point")-sum("point")).alias("No_of_loss")
                )
)
# final_df.show()