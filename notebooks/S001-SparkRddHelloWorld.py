# Databricks notebook source
# this is spark notebook/spark application
# each spark application shall have a spark context known as spark driver
# spark context
# Master - Mode is local [8], local means a mode that runs spark driver, executor in single JVM, no worker, only dev
# [8] number of parallel tasks can be performed at any point of time
# [8] can be derived by scaling number of CPUs, Number of IOs you may have in your application
# this vm/community vm, has 2 v.core, databricks initialized notebook with 8 parallel task like 1 v.core => 4 tasks
sc

# COMMAND ----------

print ("min partition ", sc.defaultMinPartitions) # useful for data reading from hdfs default partition
print ("min parallism ", sc.defaultParallelism) # number of parallell tasks can be executed at any point of time

# COMMAND ----------

# create a rdd 
# parallelize is used to load data from driver/notebook/applicaiton into spark partitions
# parallelize creates and returns RDD reference
# RDD consists of few transformation, like a tree structure, each transformation shall be applied on partition
# parallelize is lazy evaluation, means the data is not loaded into executors when we run this code until
# we apply ACTION
# RDD always remains in spark driver, never given to executors
numbersRdd = sc.parallelize([1,2,3,4,5,6,7,8,9])

# COMMAND ----------

# transformation
# data cleansing/conversion/fix
# filter, take a predicate, a function that returns true or false, filter pick the elements that returns true
# returns rdd, filtered output, returns only odd number
# filter - lazy evaluation, no code is executed 
# numbersRdd is parent, oddRdd is child - Lineage, Tree mode
oddRdd = numbersRdd.filter(lambda n: n % 2  == 1)

# COMMAND ----------

# ACTION
# Action creates a JOB
# JOB converts RDD lineage/tree into Graph model known as DAG, Directed Acylic Graph
# DAG is planned to be executed in STAGE(s)
# Each dag/stage is executed by DAG Scheduler
# each stage shall consist of TASKs
# TASK shall be given to EXECUTOR to run in parallel way, scheduled by TASK SCHEDULER WITH IN DRIVER
# EXECUTOR will apply task on each PARTITION
# Finally output is produced, OUTPUT may be returned back to DRIVER [bad approach] or OUTPUT can be stored in to Data Lake, Database, by executor

# collect results from all paritions, bring in back to Driver, not to be used in production
results = oddRdd.collect()
print(results)
# notice, there is Spark Job below cell, but it was not present for parallize, transformation function
# Job 0 - Job id
# Stages 1 stage, Stage ID is 0
# Stage 0: 2/8, Total 8 tasks

# COMMAND ----------

# every time, we apply ACTON, may be the ACTION too, it will create JOB
# aciton called 2 times
results = oddRdd.collect()
results2 = oddRdd.collect()

# COMMAND ----------

