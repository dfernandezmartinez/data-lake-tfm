from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession, SQLContext, DataFrame
from pyspark.sql.functions import col, udf, lit, sum
from pyspark.sql.types import IntegerType
import time
from delta import *

### Params ###
date = "2023-02-01"
mariadb_ip = "***"
cassandra_ip = "***"
cassandra_ip2 = "***"
cassandra_ip3 = "***"

### Spark setup ###
spark = SparkSession.builder \
    .appName("TestKpis") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.hadoop.fs.s3a.block.size","65000") \
    .config("spark.hadoop.fs.s3a.fast.upload","true") \
    .config("spark.hadoop.fs.s3a.access.key", "***") \
    .config("spark.hadoop.fs.s3a.secret.key", "***") \
    .config("spark.hadoop.fs.s3a.endpoint", "***") \
    .config("spark.databricks.delta.retentionDurationCheck.enabled", "false") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.driver.extraClassPath", "/opt/spark-external-deps/jars/*") \
    .config("spark.executor.extraClassPath", "/opt/spark-external-deps/jars/*") \
    .getOrCreate()
sc = spark.sparkContext
start = time.time()

### Cassandra - Count sensor measures ###
def cassandra_count_partition(iterator):
    from cassandra.cluster import Cluster
    cluster = Cluster([cassandra_ip, cassandra_ip2, cassandra_ip3])
    session = cluster.connect('iotsens')
    for df in iterator:
        cassandra_query = "SELECT count(*) FROM events_by_entity_sensor_and_date WHERE tenant_id={tenant_id} AND entity_id={entity_id} AND sensor_id={sensor_id} AND date='{date}'".format(
            tenant_id=df.tenant_uuid, entity_id=df.device_uuid, sensor_id=df.measurement_uuid, date=date)
        row = session.execute(cassandra_query).one()
        yield row.count
    session.shutdown()


### MariaDB - Get tenants DF ###
query_tenants = """SELECT uuid , name FROM tenant"""
tenants_df = spark.read \
    .format("jdbc").option("url", "jdbc:mysql://" + mariadb_ip + "/iotsensCatalog") \
    .option("driver", "com.mysql.cj.jdbc.Driver").option("user", "***").option("password", "***") \
    .option("query", query_tenants).load()

result = []
for tenant in tenants_df.toLocalIterator():
    query_events = """
        SELECT t.name as tenant_name, t.uuid as tenant_uuid, d.uuid as device_uuid, dm.uuid as measurement_uuid 
        FROM `trigger` AS TR JOIN device_measurement AS dm ON TR.device_measurement_id = dm.id 
        JOIN device AS d ON d.id=dm.device_id JOIN tenant as t ON d.tenant_id=t.id
        WHERE TR.device_measurement_id is NOT NULL AND TR.last_evaluation IS NOT NULL AND t.name='{name}' GROUP BY TR.device_measurement_id
        """.format(name=tenant.name)

    mariadb_df = spark.read \
        .format("jdbc").option("url", "jdbc:mysql://" + mariadb_ip + "/iotsensCatalog") \
        .option("driver", "com.mysql.cj.jdbc.Driver").option("user", "***").option("password", "***") \
        .option("query", query_events).load().repartition(4)
    mariadb_df_count = mariadb_df.rdd.cache().count()
    total_events = mariadb_df.rdd.mapPartitions(cassandra_count_partition).sum()

    result.append((tenant.name, date, total_events))
    print("Tenant: ", tenant, " [OK]")

result_df = spark.createDataFrame( result, ["tenant", "date", "total_events"])
EVENTS_TABLE_STORAGE_PATH = "s3a://iotsens/gold/total_events"
result_df.write.format("delta").mode("append").save(EVENTS_TABLE_STORAGE_PATH)
