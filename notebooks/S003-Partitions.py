# Databricks notebook source
# how to increase/decrease partitions

numbersRdd = sc.parallelize(range(1, 51), 2) # 2 is number of partitions
print("partitions ", numbersRdd.getNumPartitions())
numbersRdd.glom().collect()

# COMMAND ----------

# increase partitions using repartition, involes shuffling data across partition
rdd2 = numbersRdd.repartition(4)
print("partitions ", rdd2.getNumPartitions())
rdd2.glom().collect()

# COMMAND ----------

# reduce the number of partitions
# coalesce to reduce the number of partitions, try its best to minimize shuffling
rdd3 = rdd2.coalesce(1)
print("partitions ", rdd3.getNumPartitions())
rdd3.glom().collect()

# COMMAND ----------

