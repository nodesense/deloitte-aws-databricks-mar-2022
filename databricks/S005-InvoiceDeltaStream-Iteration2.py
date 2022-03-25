# Databricks notebook source
checkpoint_path = '/tmp/delta-gks3/invoices/_checkpoints'

# in the place of gks, use your s3 mount
# where output goes, delta files in parquet format, _delta_logs/*.json
write_path = '/mnt/gks/delta-gks3/invoices'

# Input data
upload_path = "s3://trainingmar22-invoices/invoices/"

# COMMAND ----------

# Set up the stream to begin reading incoming files from the
# upload_path location.
# {"InvoiceNo": 225124, "StockCode": "85123A", "Quantity": 10, 
# "Description": "TODO", "InvoiceDate": "03/25/2022 15:13", 
# "UnitPrice": 2.0, "CustomerID": 12583, "Country": "BE"}

df = spark.readStream.format('cloudFiles') \
  .option('cloudFiles.format', 'json') \
  .option('header', 'true') \
  .schema('InvoiceNo string, StockCode string,  Description string,    InvoiceDate string, UnitPrice double, CustomerID long,  Country string, Quantity long') \
  .load(upload_path)

# .schema('city string, year int, population long') \
# lazy avaluation, will not start stream

# COMMAND ----------

import pyspark.sql.functions as F
dfWithAmount = df.withColumn("Amount", F.col("UnitPrice") * F.col("Quantity") )
dfWithAmount.printSchema()

# COMMAND ----------

# Start the stream.
# Use the checkpoint_path location to keep a record of all files that
# have already been uploaded to the upload_path location.
# For those that have been uploaded since the last check,
# write the newly-uploaded files' data to the write_path location.
query = dfWithAmount.writeStream.format('delta') \
  .option('checkpointLocation', checkpoint_path) \
  .start(write_path)

# this starts the query, ACTION on the query
# this will compile using catalyst and execute the plan tongston on phase 1, phase 2....
# by default there will be trigger, trigger as soon as the data is available

# COMMAND ----------

# to check the output in the delta location, this is batch not stream
invoicesDf = spark.read.format('delta').load(write_path)

display(invoicesDf)

# COMMAND ----------

import pyspark.sql.functions as F
# 03/25/2022 15:13
# MM/dd/yyyy HH:mm
byCountry5MinDf = dfWithAmount.withColumn("timestamp", F.to_timestamp("InvoiceDate", "MM/dd/yyyy HH:mm"))\
                              .withWatermark("timestamp", "1 minutes")\
                              .groupBy("Country", F.window(F.col("timestamp"), "5 minutes") )\
                              .agg(F.sum("Amount").alias("TotalAmount"))\
                              .select("*", F.col("window.*"))\
                               .drop("window") 
    


byCountry5MinDf.printSchema()

# COMMAND ----------

checkpoint_path = '/tmp/delta-gks3/invoices-window/_checkpoints'

# in the place of gks, use your s3 mount
# where output goes, delta files in parquet format, _delta_logs/*.json
write_path = '/mnt/gks/delta-gks3/invoices-amount-countrywise'

query = byCountry5MinDf.writeStream.format('delta') \
  .option('checkpointLocation', checkpoint_path) \
  .start(write_path)

# COMMAND ----------

# to check the output in the delta location, this is batch not stream
groupDf = spark.read.format('delta').load(write_path)

display(groupDf)

# COMMAND ----------

# without water marking with update mode

import pyspark.sql.functions as F
# 03/25/2022 15:13
# MM/dd/yyyy HH:mm
byCountry5MinDf = dfWithAmount.withColumn("timestamp", F.to_timestamp("InvoiceDate", "MM/dd/yyyy HH:mm"))\
                              .groupBy("Country", F.window(F.col("timestamp"), "5 minutes") )\
                              .agg(F.sum("Amount").alias("TotalAmount"))\
                              .select("*", F.col("window.*"))\
                               .drop("window") 
    


byCountry5MinDf.printSchema()

checkpoint_path = '/tmp/delta-gks3/invoices-window-no-watermark/_checkpoints'

# in the place of gks, use your s3 mount
# where output goes, delta files in parquet format, _delta_logs/*.json
write_path = '/mnt/gks/delta-gks3/invoices-amount-countrywise-no-watermark'

query = byCountry5MinDf.writeStream.format('delta') \
  .outputMode("complete")\
  .option('checkpointLocation', checkpoint_path) \
  .start(write_path)

# COMMAND ----------

# to check the output in the delta location, this is batch not stream
groupDf = spark.read.format('delta').load(write_path)

display(groupDf)

# COMMAND ----------

