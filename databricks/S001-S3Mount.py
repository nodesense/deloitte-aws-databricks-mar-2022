# Databricks notebook source
aws_bucket_name = "yyyyyyyyy-bucket"
mount_name = "gks" # a virtual volume/directory that links S3 bucket
# if we access /mnt/gks, this refers to gks-bucket
# access files /mnt/gks   or dbfs://mnt/gks
dbutils.fs.mount("s3a://%s" % aws_bucket_name, "/mnt/%s" % mount_name)
display(dbutils.fs.ls("/mnt/%s" % mount_name))

# COMMAND ----------

display(dbutils.fs.ls("/mnt/%s" % mount_name))

# COMMAND ----------

aws_bucket_name = "xxxxxxxxxx-bucket"
mount_name = "test" # a virtual volume/directory that links S3 bucket
# if we access /mnt/gks, this refers to gks-bucket
# access files /mnt/gks   or dbfs://mnt/gks
dbutils.fs.mount("s3a://%s" % aws_bucket_name, "/mnt/%s" % mount_name)
display(dbutils.fs.ls("/mnt/%s" % mount_name))

# COMMAND ----------

