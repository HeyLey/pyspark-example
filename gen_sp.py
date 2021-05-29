import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import HiveContext
from pyspark import SparkContext, SparkConf
from pyspark.sql.types import StructType, IntegerType, StringType, TimestampType
import os

conf = (SparkConf()
  .setAppName('my_app')
  .set('spark.jars.packages','ru.yandex.clickhouse:clickhouse-jdbc:0.2.4,org.apache.hadoop:hadoop-aws:3.2.2,com.google.guava:guava:30.0-jre')
  .set('spark.jars.excludes','org.slf4j:slf4j-api')
  .set('spark.executor.userClassPathFirst','true')
  .set('spark.driver.userClassPathFirst','true')
  .set('spark.driver.extraClassPath', 'udf_2.12-1.0.jar')
)

sc=SparkContext(conf=conf)


hadoopConf = sc._jsc.hadoopConfiguration()
hadoopConf.set('fs.s3a.access.key', accessKeyId)
hadoopConf.set('fs.s3a.secret.key', secretAccessKey)
hadoopConf.set('fs.s3a.impl', 'org.apache.hadoop.fs.s3a.S3AFileSystem')

spark=SparkSession(sc)
spark.udf.registerJavaFunction("get_user_id", "GetUserId", StringType())

print("STARTING TO READ CSV")

schema = StructType() \
      .add("timestamp",StringType(),True) \
      .add("ip",StringType(),True) \
      .add("city",StringType(),True) \
      .add("country",StringType(),True) \
      .add("url",StringType(),True) \
      .add("hz",StringType(),True) 

df=spark.read.csv('',sep=',', header=False, schema=schema)#.toDF("timestamp","ip","city", "country", "url", "hz") # inferSchema=True)
df.show(5)

print("ADD USER_ID")
df.createOrReplaceTempView("data")
df2 = spark.sql("SELECT *, get_user_id(data.ip) as user_id FROM data limit 100")
#df2 = spark.sql("SELECT * FROM data limit 100")
df2.show(5)

print("ISERT INTO CLICKHOUSE")

url = 'jdbc:clickhouse://116.202.192.202:54345/datos'

properties = {
    "isolationLevel": "NONE",
    "max_partitions_per_insert_block": "100000000",
    "max_parts_in_total": "10000000",
    "driver": "ru.yandex.clickhouse.ClickHouseDriver"
}

df2.write.jdbc(url=url, table="tmp_106", mode="append",properties=properties)

print("DONE")
