# Databricks notebook source
# Step 1: VPC Peering - done
# Step 2: JDBC Jar for Redshift as solution for java.lang.ClassNotFoundException: com.amazon.redshift.jdbc42.Driver
# Step 3: JDBC URL username/password

# COMMAND ----------

# install library  com.amazon.redshift:redshift-jdbc42:2.1.0.5   from mvn
# WRITE TO JDBC, Just an example
# it will create an employee table and write content
data = [ ("James", "Sales", 3000),
    ("Michael", "Sales", 4600),
    ("Robert", "Sales", 4100),
    ("Maria", "Finance", 3000),
    ("James", "Sales", 3000),
    ("Scott", "Finance", 3300),
    ("Jen", "Finance", 3900),
    ("Jeff", "Marketing", 3000),
    ("Kumar", "Marketing", 2000),
    ("Saif", "Sales", 4100)
   ]

empDf = spark.createDataFrame(data=data, schema=['name', 'dept', 'salary'])
empDf.printSchema()
empDf.show()

( empDf
.write
 .mode("overwrite")
.format("jdbc")
.option("url", "jdbc:redshift://redshift-cluster-1.yyyyyyyyy.us-east-2.redshift.amazonaws.com:5439/dev")
.option("driver", "com.amazon.redshift.jdbc42.Driver")
.option("user", "awsuser")
.option("password", "xxxxxxxxx")
.option("dbtable", "gks_employees")
 .save()
)

# after sucessful exection, you can check redshift for table

# COMMAND ----------


# READING FROM POSTGRESQL from RDS
emp2 = ( spark.read
.format("jdbc")
.option("url", "jdbc:redshift://redshift-cluster-1.yyyyyyyyy.us-east-2.redshift.amazonaws.com:5439/dev")
.option("driver", "com.amazon.redshift.jdbc42.Driver")
.option("user", "awsuser")
.option("password", "yyyyyyyyy")
.option("dbtable", "gks_employees")
 .load()
 )

emp2.printSchema()
emp2.show(5)

# COMMAND ----------

# READING FROM POSTGRESQL from RDS
# read from redshift using postgresql driver, don't use it for production

emp2 = ( spark.read
.format("jdbc")
.option("url", "jdbc:postgresql://redshift-cluster-1.yyyyyyyyy.us-east-2.redshift.amazonaws.com:5439/dev")
.option("driver", "org.postgresql.Driver")
.option("user", "awsuser")
.option("password", "yyyyyyyyy")
.option("dbtable", "gks_employees")
 .load()
 )

emp2.printSchema()
emp2.show(5)

# COMMAND ----------


# READING FROM POSTGRESQL from RDS
emp2 = ( spark.read
.format("jdbc")
.option("url", "jdbc:redshift://redshift-cluster-1.yyyyyyyyy.us-east-2.redshift.amazonaws.com:5439/dev")
.option("driver", "com.amazon.redshift.jdbc42.Driver")
.option("user", "awsuser")
.option("password", "yyyyyyyyy")
.option("dbtable", "gks.popular_movies")
 .load()
 )

emp2.printSchema()
emp2.show(5)

# COMMAND ----------

