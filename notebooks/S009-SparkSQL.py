# Databricks notebook source
# Spark Database can be classified as OLAP - Online Analytical Processing
# Spark act as Query Engine 
# Hive Meta Store act as Meta data engine [database, tables, columns, locations mapping to s3/hdfs etc]

# COMMAND ----------

# database meta data shall be with HIVE
# database files shall be with DBFS/S3/HDFS
# /user/hive/warehouse/moviedb.db
spark.sql ("CREATE DATABASE IF NOT EXISTS moviedb")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- table meta data shall be with HIVE
# MAGIC -- table data files shall be with DBFS/S3/HDFS
# MAGIC -- /user/hive/warehouse/moviedb.db/reviews
# MAGIC -- where we can write sql code in python notebook
# MAGIC -- any sql written here shall be taken and executed inside spark.sql, result shall be displayed in display/html output
# MAGIC -- managed table, delta format
# MAGIC -- managed table - insert/update/delete should be done via spark sql
# MAGIC 
# MAGIC CREATE TABLE IF NOT EXISTS moviedb.reviews (movieId INT, description STRING)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- insert record
# MAGIC INSERT INTO moviedb.reviews VALUES(1, 'funny')

# COMMAND ----------

# MAGIC %sql 
# MAGIC SELECT * FROM moviedb.reviews

# COMMAND ----------

# MAGIC %sql
# MAGIC update moviedb.reviews set description='comedy' where movieId=1

# COMMAND ----------

# MAGIC %sql 
# MAGIC SELECT * FROM moviedb.reviews

# COMMAND ----------

# MAGIC %sql
# MAGIC DELETE FROM moviedb.reviews where movieid=1

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM moviedb.reviews

# COMMAND ----------

