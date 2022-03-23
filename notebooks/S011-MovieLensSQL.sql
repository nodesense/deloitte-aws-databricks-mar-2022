-- Databricks notebook source
SHOW DATABASES

-- COMMAND ----------

-- show tables from default db
SHOW TABLES 

-- COMMAND ----------

SHOW TABLES IN moviedb

-- COMMAND ----------

-- desc, reviews is managed table
-- managed means, insert, update, delete should happend via Spark DML statement
DESC FORMATTED moviedb.reviews

-- COMMAND ----------

-- desc, reviews is external table
-- external means, we can add remove files in the directory using ETL, manually
DESC FORMATTED moviedb.movies_csv

-- COMMAND ----------

SELECT count(*) from moviedb.movies_csv

-- COMMAND ----------

SELECT * from moviedb.movies_csv where movieId=100001

-- COMMAND ----------

SELECT count(*) from moviedb.ratings_csv

-- COMMAND ----------

SELECT movieId, upper(title) FROM moviedb.movies_csv LIMIT 15

-- COMMAND ----------

-- MAGIC %python 
-- MAGIC spark.sql("SELECT movieId, upper(title) FROM moviedb.movies_csv ORDER BY title LIMIT 5").show(truncate=False)

-- COMMAND ----------

SELECT movieId, upper(title) FROM moviedb.movies_csv ORDER BY title 
DESC LIMIT 5)

-- COMMAND ----------

SELECT rating, rating + 0.4 from moviedb.ratings_csv LIMIT 5

-- COMMAND ----------

SELECT movieId, avg(rating) as avg_rating, count(userId) as vote_counts
FROM moviedb.ratings_csv 
GROUP BY movieId
HAVING vote_counts >= 100 AND avg_rating >= 3.5
ORDER BY avg_rating DESC

-- COMMAND ----------

SHOW TABLES

-- COMMAND ----------

-- temp view is in memory, applicable only with in spark application
-- only created default db
CREATE OR REPLACE TEMP VIEW popular_movies AS 
SELECT movieId, avg(rating) as avg_rating, count(userId) as vote_counts
FROM moviedb.ratings_csv 
GROUP BY movieId
HAVING vote_counts >= 100 AND avg_rating >= 3.5
ORDER BY avg_rating DESC

-- COMMAND ----------

SHOW TABLES

-- COMMAND ----------

SELECT * FROM popular_movies 

-- COMMAND ----------

-- MAGIC %python
-- MAGIC dbutils.fs.rm("/FileStore/tables/ml-latest-small/movies.csv")

-- COMMAND ----------

ALTER TABLE moviedb.movies_csv SET LOCATION "/FileStore/tables/ml-latest-small/movies"

-- COMMAND ----------

DESC FORMATTED moviedb.movies_csv

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW most_popular_movies AS 
SELECT movies.movieId, title, avg_rating, vote_counts FROM popular_movies
INNER JOIN moviedb.movies_csv movies ON movies.movieId = popular_movies.movieId
ORDER BY vote_counts DESC

-- COMMAND ----------

SELECT * FROM most_popular_movies LIMIT 10

-- COMMAND ----------

-- create a permanent table from sql

CREATE OR REPLACE TABLE moviedb.must_watch_movies AS 
SELECT movies.movieId, title, avg_rating, vote_counts FROM popular_movies
INNER JOIN moviedb.movies_csv movies ON movies.movieId = popular_movies.movieId
ORDER BY vote_counts DESC

-- COMMAND ----------

SELECT * FROM moviedb.must_watch_movies

-- COMMAND ----------

