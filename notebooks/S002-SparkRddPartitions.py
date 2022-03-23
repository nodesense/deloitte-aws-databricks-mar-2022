# Databricks notebook source
# each rdd has partitions, lazy evaluation, 
# partitions created only when we apply ACTIONS

numbersRdd = sc.parallelize(range(1, 21))

numbersRdd.getNumPartitions() # 8, parallelize func takes default partitions from minParallism attribute

# COMMAND ----------

# collect, collect data from all partitions, merge them together
numbersRdd.collect()

# COMMAND ----------

# glom , ACTION method
# collect data from each partition, doesn't merge
numbersRdd.glom().collect()

# COMMAND ----------

# take, ACTION
# take reads first few elements from partition 0.. onwards
# best method for sampling, print output to check outputs
numbersRdd.take(4) # take 4 elements, it picks 2 elements from partition 0, picks 2 elements from partition 1..

# COMMAND ----------

# to process each element in the partition one by one, foreach

def process(n):
  print("Number is ", n)
  # todo process individual element in the partition using custom functions
# foreach is higher order funciton , a function that accept another function as input
# spark will call process function for each element in the partition

# foreach is ACTION method, output shall be in SPARK cluster, not displayed in notebook
# print statement is running on executor, not on driver
# Cluster=> Click on running cluster ==> Spark Logs/Output
numbersRdd.foreach(process)

# COMMAND ----------

# foreachPartition , Action function 
# it perform an action on a funciton, with whole partition data
# itr = iterator, lazy evaluation, we can able to iterate in loop to fetch data one by one
# executed inside executor, logs should be available in spark logs in compute cluster
# 
def processPartition(itr):
  print("-----------------")
  for n in itr:
    print("Number is ", n)
    
  print("---------------")
  
numbersRdd.foreachPartition(processPartition)

# COMMAND ----------

