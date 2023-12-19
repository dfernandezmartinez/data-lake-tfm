import pyspark
from delta import *

from pyspark.sql.functions import *
import json
from kafka3 import KafkaConsumer

builder = pyspark.sql.SparkSession.builder.appName("Kafka consumer standallone")
spark = configure_spark_with_delta_pip(builder).getOrCreate()

DEBEZIUM_TABLE_STORAGE_PATH = "***"

spark.sql("DROP TABLE IF EXISTS debezium")

spark.sql(f"""
  CREATE TABLE debezium
    (
      at LONG NOT NULL,
      op STRING NOT NULL,
      id LONG NOT NULL,
      uuid STRING NOT NULL,
      task_name STRING NOT NULL,
      timestamp STRING NOT NULL
    )
    USING DELTA
    PARTITIONED BY (id)
    LOCATION '{DEBEZIUM_TABLE_STORAGE_PATH}'
""")

MATCH_TABLE_MAP = {
        "at": "s.at",
        "op": "s.op",
        "id": "s.id",
        "uuid": "s.uuid",
        "task_name": "s.task_name",
        "timestamp": "s.timestamp"
}

debeziumTable = DeltaTable.forPath(spark, DEBEZIUM_TABLE_STORAGE_PATH)
consumer = KafkaConsumer('iotsens.iotsensCatalog.debezium', bootstrap_servers=['***'])
for message in consumer:
    if message.value is not None:
        print("Message proceeded")
        print(message)
        jsonRDD = spark.sparkContext.parallelize([message.value.decode("utf8")])
        prefix = "payload.before" if json.loads(message.value.decode("utf8"))['payload']['op'] == 'd' else "payload.after"
        test = spark.read.json(jsonRDD).select(
            col("payload.ts_ms").alias("at"),
            col("payload.op").alias("op"),
            col("{}.id".format(prefix)).alias("id"),
            col("{}.uuid".format(prefix)).alias("uuid"),
            col("{}.task_name".format(prefix)).alias("task_name"),
            col("{}.timestamp".format(prefix)).alias("timestamp"))
        (debeziumTable.alias("t").merge(test.alias("s"), "t.id = s.id")
         .whenMatchedDelete(condition="s.op = 'd'")
         .whenMatchedUpdate(set=MATCH_TABLE_MAP)
         .whenNotMatchedInsert(condition="s.op != 'd'", values=MATCH_TABLE_MAP)
         .execute())
    else:
        print("Message is not valid")
