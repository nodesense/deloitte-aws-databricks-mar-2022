# Databricks notebook source
# map, transformation method
# useful for data conversion
rdd = sc.parallelize(range(1, 6))

# now multiple all numbers by 10
rdd2 = rdd.map (lambda n : n  * 10)
# result shall be numbers 10, 20..50
rdd2.collect()

# COMMAND ----------

# FLAT MAP

# List of List of elements
data = [
    [1,2,3],
    [4,5,6],
    [7,8,9]
]

rdd = sc.parallelize(data)

print("Count ", rdd.count())
rdd.collect()

# COMMAND ----------

# flatten the list, we want the individual element from list, to be projected as separate element or record

# flatMap
# flatten elements inside array into elements
# remove the list, project elements as records
# each element in the list will be 1 record

rddFlatMap = rdd.flatMap (lambda x: x) 

print ("Count ", rddFlatMap.count())

rddFlatMap.collect()

# COMMAND ----------

# Key Value pair formed from tuple, where as first element in tuple is known as key
# second element known as value
# (key, value)
# apple is key, 20 is value
data = [
    ('apple', 20),
    ('orange', 30),
    ('apple', 10),
    ('mango', 50)
]

rdd = sc.parallelize(data)

# COMMAND ----------

# find the count keys
# for rdd functions with "Key" can use this dataset

result = rdd.countByKey() # action
result # result is dictionary, it returns count of keys

# COMMAND ----------

# aggregation, we want sum the fruits sold ie sum of values for each type of fruit
"""
INPUT:
  ('apple', 20), <-- first apple key present, reduceByKey won't call lambda, instead initialize key value in table
  ('orange', 30), <- first time orange appear, initialized in table , won't call lambda
  ('apple', 10), <-- apple appear second time, reduceByKey will call the lambda for aggregation
                       call lambda, acc value picked from table
                       lambda (20/acc, 10/value) => 20 + 10 = 30, then 30 is updated in the internal state
  ('mango', 50) <- first time mango, initialized in table

reduceByKey: Internal State/table

Key     Acc

apple   30  [was 20 before]
orange  30
mango   50

"""
# acc is just variable, called accumulator
# value is from rdd, 20, 30, 10 ,50
# reducebyKey shall call lambda acc, value: acc + value, internally and get the output, accumulate result, finally
# return return, reduceByKey is transformation
result = rdd.reduceByKey(lambda acc, value: acc + value)
result.collect()

# COMMAND ----------

# joins

sectors = [
    # tuple, the key is used for join purpose
    # sector id, sector name
    (0, "AUTOMOTIVE"),
    (1, "TEXTILE"),
    (2, "IT"),
    (4, "MANUFACTURING")
]

stocks = [
    # key   is used for join purpose
    
    # key is 0, 1, 4 and value is  ("TSLA", 100), SYM1, SYM2
    # sector id, symbol, price
    (0, ("TSLA", 100) ),
    (0, ("GM", 40 ) ),
    (1, ("SYM1", 45) ),
    (4, ("SYM2", 67) )
]

sectorRdd = sc.parallelize(sectors)
stocksRdd = sc.parallelize(stocks)
# join based on rdd keys it has to match sectorRdd sector id with stocksRdd sector id
joinRdd = sectorRdd.join(stocksRdd)

joinRdd.collect()

# COMMAND ----------

joinRdd = sectorRdd.join(stocksRdd)\
                   .map(lambda record: (record[1][0], ) + record[1][1])

joinRdd.collect()

# COMMAND ----------

## BroadCast

sector_dict = {
        "MSFT": "TECH",
        "TSLA": "AUTO",
        "EMR": "INDUSTRIAL"
}

stocks = [
    ("EMR", 52.0),
    ("TSLA", 300.0),
    ("MSFT", 100.0)
]

# COMMAND ----------

# create broadcasted variabel using Spark Context
# this will ensure that sector_dict is kept in every executor 
# where task shall be running
# lazy evaluation, data shall be copied to executors when we run the job
broadCastSectorDict = sc.broadcast(sector_dict)

# COMMAND ----------

stocksRdd = sc.parallelize(stocks)

# COMMAND ----------

# Pyspark see this code, this has reference to broadCastSectorDict
# which is broardcast data, pyspark place the broadCastSectorDict in every executor
# 1 time instead of every job
# without broadCast, sector_dict shall be copied to executor for every task
# add sector code with stock at executor level when task running
def enrichStockWithSector(stock):
    return stock + (broadCastSectorDict.value[stock[0]] ,)

# COMMAND ----------

# code marshalling - Python copy the lamnda code to executor system/processor
# now enrichStockWithSector also shipped to executor on every task
enrichedRdd = stocksRdd.map (lambda stock: enrichStockWithSector(stock))

# COMMAND ----------

enrichedRdd.collect()


# COMMAND ----------

# is rdd empty or not

enrichedRdd.isEmpty() # False

# COMMAND ----------

# SET Example

# here general rdd apis

dayTradeSymbols = sc.parallelize(['INFY', 'MINDTREE', 'TSLA'])
swingTradeSymbols = sc.parallelize(['EMR', 'AAPL', 'TSLA', 'INFY'])

# COMMAND ----------

# return the elements/stock that are present in both
intersectionRdd = dayTradeSymbols.intersection(swingTradeSymbols)
intersectionRdd.collect()

# COMMAND ----------

# union of two RDD, add items from both the RDD, duplicates possible
unionRdd = dayTradeSymbols.union(swingTradeSymbols)
unionRdd.collect()

# COMMAND ----------

# distrinct , remove duplicates 
# unionRdd has duplicates TSLA, INFY coming twice
distinctRdd = unionRdd.distinct()
distinctRdd.collect()

# COMMAND ----------

