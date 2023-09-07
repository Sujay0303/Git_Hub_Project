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

df1=spark.read.json("s3://input-datapipeline/main/part-00000-b6113063-fbed-4d1a-b559-1843a791f443-c000.json")
df2 = spark.read.format("jdbc").option("url", "jdbc:mysql://github-database.cpirtk1qzqlt.us-east-1.rds.amazonaws.com:3306/projectdb").option("dbtable", "maintable").option("user", "admin").option("password", "project1").load()

df3=spark.read.json("s3://input-datapipeline/repo/part-00000-8d51e744-d341-40b5-b160-f06ee7ece848-c000.json")
df4 = spark.read.format("jdbc").option("url", "jdbc:mysql://github-database.cpirtk1qzqlt.us-east-1.rds.amazonaws.com:3306/projectdb").option("dbtable", "repotable").option("user", "admin").option("password", "project1").load()

df5=spark.read.parquet("s3://input-datapipeline/commit/part-00000-e66ae499-185d-4a0b-bb1f-0d79f810cc09-c000.snappy.parquet")
df6 = spark.read.format("jdbc").option("url", "jdbc:mysql://github-database.cpirtk1qzqlt.us-east-1.rds.amazonaws.com:3306/projectdb").option("dbtable", "committable").option("user", "admin").option("password", "project1").load()

maindf = df1.unionByName(df2)
maindf.coalesce(1).write.parquet("s3://group04datalake/datawarehouse/maintable")

repodf = df3.unionByName(df4)
repodf.coalesce(1).write.parquet("s3://group04datalake/datawarehouse/repotable")

commitdf = df5.unionByName(df6)
commitdf.coalesce(1).write.parquet("s3://group04datalake/datawarehouse/committable")  

job.commit()