# Databricks notebook source
# /FileStore/tables/ml-latest-small/movies.csv
# /FileStore/tables/ml-latest-small/ratings.csv

# COMMAND ----------

# how to create schema programatically instead of using inferSchema
from pyspark.sql.types import StructType, LongType, StringType, IntegerType, DoubleType
# True is nullable, False is non nullable
movieSchema = StructType()\
                .add("movieId", IntegerType(), True)\
                .add("title", StringType(), True)\
                .add("genres", StringType(), True)

ratingSchema = StructType()\
                .add("userId", IntegerType(), True)\
                .add("movieId", IntegerType(), True)\
                .add("rating", DoubleType(), True)\
                .add("timestamp", LongType(), True)

# COMMAND ----------

# read movie data
# read using dataframe with defind schema
# we can use folder path - all csv in the folder read
# use file path, only that file read

# spark is session, entry point for data frame/sql

movieDf = spark.read.format("csv")\
                .option("header", True)\
                .schema(movieSchema)\
                .load("/FileStore/tables/ml-latest-small/movies.csv")

movieDf.printSchema()
movieDf.show(2)

# COMMAND ----------

ratingDf = spark.read.format("csv")\
                .option("header", True)\
                .schema(ratingSchema)\
                .load("/FileStore/tables/ml-latest-small/ratings.csv")

ratingDf.printSchema()
ratingDf.show(2)

# COMMAND ----------

print (movieDf.count())
print(ratingDf.count())

# COMMAND ----------

ratingDf.take(2)

# COMMAND ----------

# show the distinct ratings
ratingDf.select("rating").distinct().sort("rating").show()

# COMMAND ----------

# aggregation with groupBy
from pyspark.sql.functions import col, desc, avg, count

# find the movies by total ratings by userId
df = ratingDf\
     .groupBy("movieId")\
     .agg(count("userId").alias("total_ratings"))\
     .sort(desc("total_ratings"))

df.printSchema()
df.show(20)

# COMMAND ----------

# aggregation with groupBy
from pyspark.sql.functions import col, desc, avg, count

# find  average rating by users sorted by desc
df = ratingDf\
     .groupBy("movieId")\
     .agg(avg("rating").alias("avg_rating"))\
     .sort(desc("avg_rating"))

df.printSchema()
df.show(20)

# COMMAND ----------

# aggregation with groupBy
from pyspark.sql.functions import col, desc, avg, count

# find  the most popular movies, where as rated by many users, at least movies should be rated by 100 users
# and the average rating should be at least 3.5 and above
# and sort the movies by total_ratings
mostPopularMoviesDf = ratingDf\
     .groupBy("movieId")\
     .agg(avg("rating").alias("avg_rating"), count("userId").alias("total_ratings") )\
     .filter( (col("total_ratings") >= 100) & (col("avg_rating") >=3.5) )\
     .sort(desc("total_ratings"))

# cache, save expensive io load, expensive computation.
# the output is cached
mostPopularMoviesDf.cache() # MEMORY

mostPopularMoviesDf.printSchema()
mostPopularMoviesDf.show(20)
print(desc("test"))

# COMMAND ----------

# aggregation with groupBy
from pyspark.sql.functions import col, desc, avg, count

# find  the most popular movies, where as rated by many users, at least movies should be rated by 100 users
# and the average rating should be at least 3.5 and above
# and sort the movies by total_ratings
mostPopularMoviesDf = ratingDf\
     .groupBy("movieId")\
     .agg(avg("rating").alias("avg_rating"), count("userId").alias("total_ratings") )\
     .filter( (col("total_ratings") >= 100) & (col("avg_rating") >=3.5) )\
     .sort(desc("total_ratings"))

mostPopularMoviesDf.cache() # MEMORY

mostPopularMoviesDf.printSchema()
mostPopularMoviesDf.show(20)

# COMMAND ----------

# join, inner join 
# get the movie title for the mostPopularMoviesDf
# join mostPopularMoviesDf with movieDf based on condition that mostPopularMoviesDf.movieId == movieDf.movieId

popularMoviesDf = mostPopularMoviesDf.join(movieDf, mostPopularMoviesDf.movieId == movieDf.movieId)\
                                     .select(movieDf.movieId, "title", "avg_rating", "total_ratings")\
                                     .sort(desc("total_ratings"))

popularMoviesDf.cache()

popularMoviesDf.show(100)

# COMMAND ----------

# write popularMoviesDf to hadoop with header [by default headers shall not be written]
# overwrite existing files
# many partitions having approx total of 100 plus records
# write many files into DBFS 
popularMoviesDf.write.mode("overwrite")\
                .option("header", True)\
                .csv("/FileStore/tables/2022-mar-21/movielens/most-popular-movies-many-files")

# COMMAND ----------

# write popularMoviesDf into single file
# coalesce(1) to reduce partitions
# overwrite - remove existing content and add new content
popularMoviesDf.coalesce(1).write.mode("overwrite")\
                .option("header", True)\
                .csv("/FileStore/tables/2022-mar-21/movielens/most-popular-movies")

# COMMAND ----------

# write popularMoviesDf into single file
# coalesce(1) to reduce partitions
# output mode : append, add files to existing folder/table

popularMoviesDf.coalesce(1).write.mode("append")\
                .option("header", True)\
                .csv("/FileStore/tables/2022-mar-21/movielens/most-popular-movies")

# COMMAND ----------

# write popularMoviesDf into single file
# coalesce(1) to reduce partitions
# output mode : ignore, if the folder/table already exist, silently ignore write operation, it won't write if table/files already present

popularMoviesDf.coalesce(1).write.mode("ignore")\
                .option("header", True)\
                .csv("/FileStore/tables/2022-mar-21/movielens/most-popular-movies")

# COMMAND ----------

# write popularMoviesDf into single file
# coalesce(1) to reduce partitions
# output mode : error, default mode if mode is not mentioned, if table/directory already exist, then it will cause error, if directory/table not exist, it will create and add files
# ERROR
popularMoviesDf.coalesce(1).write.mode("error")\
                .option("header", True)\
                .csv("/FileStore/tables/2022-mar-21/movielens/most-popular-movies")

# COMMAND ----------

# # 3 differnt files
# # differnt BL, similar to all 
# # reusable code

# def processDataBL1(dataFrame):
#   # performed in Executor
#   outputDataFrame = dataFrame.withFrame("newCol", ''''''
#   return outputDataFrame
                                        

# outputDataFrameJoined = None
# get files from directory:
#       # driver you write the logic
#   dataFrame = spark.read (filePath)...                                  
 
#   if outputDataFrameJoined is None:
#      outputDataFrameJoined = dataFrame
#   else:
#   outputDataFrameJoined = outputDataFrameJoined.union(outputDataFrame)
                                        
# processDataBL1(outputDataFrameJoined)

# COMMAND ----------

# write popularMoviesDf into parquet format
# coalesce(1) to reduce partitions
# overwrite - remove existing content and add new content
popularMoviesDf.coalesce(1).write.mode("overwrite")\
                .parquet("/FileStore/tables/2022-mar-21/movielens/most-popular-movies-parquet")

# COMMAND ----------

# write popularMoviesDf into orc format
# coalesce(1) to reduce partitions
# overwrite - remove existing content and add new content
# orc - Row Columnar format, used in hive system
popularMoviesDf.coalesce(1).write.mode("overwrite")\
                .orc("/FileStore/tables/2022-mar-21/movielens/most-popular-movies-orc")

# COMMAND ----------

# DF 
# file1.sql in some directory text file ; SELECT * From.......

# # python code to read the sql file content in Driver
# sqlString = open("file1.sql", "r").read()

# sqlSt = "SELECT * FROM TABLE"
# # now it will execute
# df = spark.sql(sqlString)




# COMMAND ----------

# write popularMoviesDf into json format
# coalesce(1) to reduce partitions
# overwrite - remove existing content and add new content
# orc - Row Columnar format, used in hive system
popularMoviesDf.coalesce(1).write.mode("overwrite")\
                .json("/FileStore/tables/2022-mar-21/movielens/most-popular-movies-json")

# { "movieId": 1, "title": "Sometime".... }
# { "movieId": 2, "title": "Some title".... }


# COMMAND ----------

spark

# COMMAND ----------

