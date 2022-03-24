import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import functions as SqlFuncs
import re
from awsglue.dynamicframe import DynamicFrame

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)



def sparkAggregate(
    glueContext, parentFrame, groups, aggs, transformation_ctx
) -> DynamicFrame:
    aggsFuncs = []
    for column, func in aggs:
        aggsFuncs.append(getattr(SqlFuncs, func)(column))
    result = (
        parentFrame.toDF().groupBy(*groups).agg(*aggsFuncs)
        if len(groups) > 0
        else parentFrame.toDF().agg(*aggsFuncs)
    )
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)


# Script generated for node Movies Parquet
MoviesParquet_node1648104136489 = glueContext.create_dynamic_frame.from_catalog(
    database="gks_db",
    table_name="silver_movies",
    transformation_ctx="MoviesParquet_node1648104136489",
)

# Script generated for node Ratings Parquet
RatingsParquet_node1648104140409 = glueContext.create_dynamic_frame.from_catalog(
    database="gks_db",
    table_name="silver_ratings",
    transformation_ctx="RatingsParquet_node1648104140409",
)

# Script generated for node movies
movies_node1648104467325 = ApplyMapping.apply(
    frame=MoviesParquet_node1648104136489,
    mappings=[
        ("movieid", "long", "movieid", "long"),
        ("title", "string", "title", "string"),
    ],
    transformation_ctx="movies_node1648104467325",
)

# Script generated for node Apply Mapping
ApplyMapping_node1648104492447 = ApplyMapping.apply(
    frame=RatingsParquet_node1648104140409,
    mappings=[
        ("userid", "long", "userid", "long"),
        ("movieid", "long", "mid", "long"),
        ("rating", "double", "rating", "double"),
    ],
    transformation_ctx="ApplyMapping_node1648104492447",
)

# Script generated for node Aggregate
Aggregate_node1648104595189 = sparkAggregate(
    glueContext,
    parentFrame=ApplyMapping_node1648104492447,
    groups=["mid"],
    aggs=[["userid", "count"], ["rating", "avg"]],
    transformation_ctx="Aggregate_node1648104595189",
)

# Script generated for node Rename Field
RenameField_node1648104826325 = RenameField.apply(
    frame=Aggregate_node1648104595189,
    old_name="`count(userid)`",
    new_name="total_ratings",
    transformation_ctx="RenameField_node1648104826325",
)

# Script generated for node aggregate rating
aggregaterating_node1648104886855 = RenameField.apply(
    frame=RenameField_node1648104826325,
    old_name="`avg(rating)`",
    new_name="avg_rating",
    transformation_ctx="aggregaterating_node1648104886855",
)

# Script generated for node Join
Join_node1648105003564 = Join.apply(
    frame1=aggregaterating_node1648104886855,
    frame2=movies_node1648104467325,
    keys1=["mid"],
    keys2=["movieid"],
    transformation_ctx="Join_node1648105003564",
)

# Script generated for node Drop Fields
DropFields_node1648105395080 = DropFields.apply(
    frame=Join_node1648105003564,
    paths=["mid"],
    transformation_ctx="DropFields_node1648105395080",
)

# Script generated for node Filter
Filter_node1648105461556 = Filter.apply(
    frame=DropFields_node1648105395080,
    f=lambda row: (row["total_ratings"] >= 100 and row["avg_rating"] >= 3.5),
    transformation_ctx="Filter_node1648105461556",
)

Filter_node1648105461556 = Filter_node1648105461556.coalesce(1)

# Script generated for node S3 bucket
S3bucket_node3 = glueContext.write_dynamic_frame.from_options(
    frame=Filter_node1648105461556,
    connection_type="s3",
    format="glueparquet",
    connection_options={"path": "s3://gks-bucket/gold/popular-movies-copy2/", "partitionKeys": []},
    format_options={"compression": "snappy"},
    transformation_ctx="Filter_node1648105461556",
)
 


job.commit()
