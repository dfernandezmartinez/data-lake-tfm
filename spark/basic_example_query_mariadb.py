from pyspark.sql import SparkSession

### Params ###
mariadb_ip = "***"
db_name = "***"

### Spark setup ###
spark = SparkSession.builder \
    .appName("TestKpis") \
    .getOrCreate()
sc = spark.sparkContext

### MariaDB - Get tenant list ###
mariadb_properties = {
    "user": "***",
    "password": "***",
    "driver": "com.mysql.cj.jdbc.Driver"
}
mariadb_url = "jdbc:mysql://{mariadb_ip}/{db_name}".format(mariadb_ip=mariadb_ip, db_name=db_name)
mariadb_df = spark.read.jdbc(url=mariadb_url, properties=mariadb_properties, table="tenant")
mariadb_df_count = mariadb_df.rdd.cache().count()
tenant_list = [row['name'] for row in mariadb_df.select("name").collect()]
