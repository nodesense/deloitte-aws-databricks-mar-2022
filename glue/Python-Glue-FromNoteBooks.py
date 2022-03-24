import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

glueContext = GlueContext(SparkContext.getOrCreate())
spark = glueContext.spark_session

MoviesParquet_node1648104136489 = glueContext.create_dynamic_frame.from_catalog(
    database="gks_db",
    table_name="silver_movies",
    transformation_ctx="MoviesParquet_node1648104136489",
)

MoviesParquet_node1648104136489.printSchema()

movieDf = MoviesParquet_node1648104136489.toDF()
movieDf.show()

RatingsParquet_node1648104140409 = glueContext.create_dynamic_frame.from_catalog(
    database="gks_db",
    table_name="silver_ratings",
    transformation_ctx="RatingsParquet_node1648104140409",
)


RatingsParquet_node1648104140409.printSchema()


# Script generated for node movies
movies_node1648104467325 = ApplyMapping.apply(
    frame=MoviesParquet_node1648104136489,
    mappings=[
        ("movieid", "long", "movie_id", "long"),
        ("title", "string", "name", "string"),
    ],
    transformation_ctx="movies_node1648104467325",
)
movies_node1648104467325.printSchema()

movies_node1648104467325.toDF().show(3)

ApplyMapping_node1648104492447 = ApplyMapping.apply(
    frame=RatingsParquet_node1648104140409,
    mappings=[
        ("userid", "long", "userid", "long"),
        ("movieid", "long", "mid", "long"),
        ("rating", "double", "rating", "double"),
    ],
    transformation_ctx="ApplyMapping_node1648104492447",
)

ApplyMapping_node1648104492447.printSchema()
ApplyMapping_node1648104492447.toDF().show(2)

from awsglue.dynamicframe import DynamicFrame
from pyspark.sql import functions as SqlFuncs

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


# Script generated for node Aggregate
Aggregate_node1648104595189 = sparkAggregate(
    glueContext,
    parentFrame=ApplyMapping_node1648104492447,
    groups=["mid"],
    aggs=[["userid", "count"], ["rating", "avg"],["rating", "min"], ["rating", "max"]  ],
    transformation_ctx="Aggregate_node1648104595189",
)


Aggregate_node1648104595189.printSchema()
Aggregate_node1648104595189.toDF().show(2)

apply_renamed_agg_node = ApplyMapping.apply(
    frame=Aggregate_node1648104595189,
    mappings=[
         ("mid", "long", "mid", "long"),
        ("count(userid)", "long", "total_ratings", "long"),
        ("avg(rating)", "double", "avg_rating", "double"),
         ("min(rating)", "double", "min_rating", "double"),
         ("max(rating)", "double", "max_rating", "double"),
    ],
    transformation_ctx="apply_renamed_agg_node",
)

apply_renamed_agg_node.printSchema()
apply_renamed_agg_node.toDF().show(2)
 

Join_node1648105003564 = Join.apply(
    frame1=apply_renamed_agg_node,
    frame2=movies_node1648104467325,
    keys1=["mid"],
    keys2=["movie_id"],
    transformation_ctx="Join_node1648105003564",
)


Join_node1648105003564.printSchema()


Join_node1648105003564.toDF().show(2)


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

Filter_node1648105461556.printSchema()
Filter_node1648105461556.toDF().show(4)

Filter_node1648105461556 = Filter_node1648105461556.coalesce(1)


# Script generated for node S3 bucket
S3bucket_node3 = glueContext.write_dynamic_frame.from_options(
    frame=Filter_node1648105461556,
    connection_type="s3",
    format="glueparquet",
    connection_options={"path": "s3://gks-bucket/gold/popular-movies-copy3/", "partitionKeys": []},
    format_options={"compression": "snappy"},
    transformation_ctx="Filter_node1648105461556",
)
 


# job.commit()


