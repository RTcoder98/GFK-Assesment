# Databricks notebook source
spark


# COMMAND ----------

# MAGIC %md
# MAGIC %fs mkdirs /FileStore/GFK_data
# MAGIC
# MAGIC %fs ls /FileStore/GFK_data

# COMMAND ----------

# MAGIC %md
# MAGIC Initialize Spark session

# COMMAND ----------

from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("MovieRatings").getOrCreate()

# COMMAND ----------

# MAGIC %md
# MAGIC Load data and Assign columnn names to data

# COMMAND ----------

movie_df = spark.read.csv("dbfs:/FileStore/GFK_data/movies.dat",header = False, sep= '::')
movie_df = movie_df.toDF("MovieID","Title","Genres")

ratings_df = spark.read.csv("dbfs:/FileStore/GFK_data/ratings.dat",header = False, sep= '::')
ratings_df = ratings_df.toDF("UserID","MovieID","Rating","Timestamp")

users_df = spark.read.csv("dbfs:/FileStore/GFK_data/users.dat", header = False, sep = '::')
users_df = users_df.toDF("UserID","Gender","Age","Occupation","Zip-Code")

# COMMAND ----------

# MAGIC %md
# MAGIC shows loaded data

# COMMAND ----------

movie_df.show(10,truncate = False)
ratings_df.show(10,truncate = False)
users_df.show(10,truncate = False)

# COMMAND ----------

# MAGIC %md
# MAGIC importing require functions

# COMMAND ----------

from pyspark.sql.functions import split,col,avg,explode

# COMMAND ----------

# MAGIC %md
# MAGIC filtering users by age 

# COMMAND ----------

filtered_user_df = users_df.filter((col("Age") >= 18) & (col("Age") <= 49))
filtered_user_df.show(10)

# COMMAND ----------

# MAGIC %md
# MAGIC filtering movie by year (> 1989)

# COMMAND ----------

movie_df1 = movie_df.withColumn("Year",split(col("Title"),"\(").getItem(1).substr(1,4).cast("int"))
filtered_movie_df = movie_df1.filter(col("Year") > 1989)
filtered_movie_df.show(10)

# COMMAND ----------

# MAGIC %md
# MAGIC Joining dataframes

# COMMAND ----------

joined_df = ratings_df.join(filtered_user_df,"UserID").join(filtered_movie_df,"MovieID")
joined_df.show(10,truncate = False)

# COMMAND ----------

# MAGIC %md
# MAGIC Handle genres

# COMMAND ----------

gen_df = joined_df.withColumn("Genre",explode(split(col("Genres"),"\\|")))
gen_df.show(10,truncate = False)

# COMMAND ----------

# MAGIC %md
# MAGIC Calculating average rating per genre per year

# COMMAND ----------

avg_ratings_df = gen_df.groupBy("Genre","Year").agg(avg("Rating").alias("AvgRating"))
avg_ratings_df.show(10,truncate = False)

# COMMAND ----------

# MAGIC %md
# MAGIC %fs mkdirs /FileStore/GFK_data/results

# COMMAND ----------

# MAGIC %md
# MAGIC saving results 

# COMMAND ----------

avg_ratings_df.write.csv("dbfs:/FileStore/GFK_data/results",header= True, mode = "Overwrite")

# COMMAND ----------

# MAGIC %fs ls /FileStore/GFK_data/results
