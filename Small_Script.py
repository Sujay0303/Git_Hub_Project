import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

## @params: [JOB_NAME]
args = getResolvedOptions(sys.argv, ['JOB_NAME'])

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

df1=spark.read.json("s3://projecthaiyemera/allone/followerslist/followerslist.json")
df2 = spark.read.format("jdbc").option("url", "jdbc:mysql://github-database.cpirtk1qzqlt.us-east-1.rds.amazonaws.com:3306/projectdb").option("dbtable", "followertable").option("user", "admin").option("password", "project1").load()

df3=spark.read.json("s3://projecthaiyemera/allone/followinglist/followinglist.json")
df4 = spark.read.format("jdbc").option("url", "jdbc:mysql://github-database.cpirtk1qzqlt.us-east-1.rds.amazonaws.com:3306/projectdb").option("dbtable", "followingtable").option("user", "admin").option("password", "project1").load()

# df5=spark.read.parquet("s3://input-datapipeline/commit/part-00000-e66ae499-185d-4a0b-bb1f-0d79f810cc09-c000.snappy.parquet")
# df6 = spark.read.format("jdbc").option("url", "jdbc:mysql://github-database.cpirtk1qzqlt.us-east-1.rds.amazonaws.com:3306/projectdb").option("dbtable", "committable").option("user", "admin").option("password", "project1").load()

followerdf = df1.unionByName(df2)
followerdf.coalesce(1).write.parquet("s3://meriwalibucket2222/followertable")

followingdf = df3.unionByName(df4)
followingdf.coalesce(1).write.parquet("s3://meriwalibucket2222/followingtable")

# commitdf = df5.unionByName(df6)
# commitdf.coalesce(1).write.parquet("s3://mergeddatar14/datawarehouse/committable")

job.commit()