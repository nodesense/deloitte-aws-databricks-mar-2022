# Databricks notebook source
checkpoint_path = '/tmp/delta-gks/population_data/_checkpoints'

# in the place of gks, use your s3 mount
# where output goes, delta files in parquet format, _delta_logs/*.json
write_path = '/mnt/gks/delta-gks/population_data'

# Input data
upload_path = "/mnt/gks/population"

# COMMAND ----------

# Set up the stream to begin reading incoming files from the
# upload_path location.
df = spark.readStream.format('cloudFiles') \
  .option('cloudFiles.format', 'csv') \
  .option('header', 'true') \
  .schema('city string, year int, population long') \
  .load(upload_path)

# lazy avaluation, will not start stream

# COMMAND ----------

# Start the stream.
# Use the checkpoint_path location to keep a record of all files that
# have already been uploaded to the upload_path location.
# For those that have been uploaded since the last check,
# write the newly-uploaded files' data to the write_path location.
query = df.writeStream.format('delta') \
  .option('checkpointLocation', checkpoint_path) \
  .start(write_path)

# this starts the query, ACTION on the query
# this will compile using catalyst and execute the plan tongston on phase 1, phase 2....
# by default there will be trigger, trigger as soon as the data is available

# COMMAND ----------

# to check the output in the delta location, this is batch not stream
df_population = spark.read.format('delta').load(write_path)

display(df_population)

# COMMAND ----------

