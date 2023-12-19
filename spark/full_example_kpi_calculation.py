# spark-submit --packages mysql:mysql-connector-java:8.0.13,com.datastax.spark:spark-cassandra-connector_2.12:3.0.1 --master local[*] test-kpis.py

from pyspark.sql import SparkSession
from cassandra.cluster import Cluster
from datetime import datetime, timedelta
from enum import Enum
import mysql.connector
import time
import pytz
import uuid
from cassandra.cluster import Cluster, ExecutionProfile, EXEC_PROFILE_DEFAULT
from cassandra.policies import WhiteListRoundRobinPolicy, DowngradingConsistencyRetryPolicy
from cassandra.query import tuple_factory
from cassandra import ConsistencyLevel

Env = Enum('Env', ['TEST', 'PRE', 'PROD'])

### Calculation params ###
start_date_str = "2023-01-01"
end_date_str = "2023-12-31"
excluded_tenants = ['ADHOC_INFORMATICA', 'AGUAS_DE_MOLTO']
env = Env.PROD

### Connection params ###
cassandra_host = None
mariadb_host = None
mariadb_port = '***'
mariadb_user = "***"
mariadb_password = "***"
if env == Env.TEST:
    cassandra_host = ["***"]
    mariadb_host = "***"
elif env == Env.PRE:
    cassandra_host = ["***"]
    mariadb_host = "***"
elif env == Env.PROD:
    cassandra_host = ["***"]
    mariadb_host = "***"
mariadb_ip = mariadb_host + ":" + mariadb_port
mariadb_url = "jdbc:mysql://" + mariadb_ip + "/iotsensCatalog?serverTimezone=Europe/Madrid"

### Other params ###
kpi_prefix = "TM_"

### Spark setup ###
spark = SparkSession.builder \
    .appName("TestKPIs") \
    .getOrCreate()
sc = spark.sparkContext
sc.setLogLevel('WARN')
start = time.time()

### Open connections ###
conn = mysql.connector.connect(host=mariadb_host, user=mariadb_user, password=mariadb_password, database="iotsensCatalog")
cursor = conn.cursor()

### Get all tenants and iterate ###
get_kpi_query = "SELECT name FROM tenant"
cursor.execute(get_kpi_query)
fetch_result = cursor.fetchall()

for row in fetch_result:
    tenant_name = row[0]
    if tenant_name in excluded_tenants:
        continue

    ### Get KPI UUID or create KPI if not found ###
    get_kpi_query = "SELECT uuid FROM kpi WHERE name = '{kpi_prefix}{tenant_name}'".format(kpi_prefix=kpi_prefix, tenant_name=tenant_name)
    cursor.execute(get_kpi_query)
    fetch_result = cursor.fetchone()
    if fetch_result is None:
        print("[TestKpis] KPI for tenant {tenant_name} not found, creating it...".format(tenant_name=tenant_name))
        kpi_uuid = uuid.uuid4()
        kpi_name = kpi_prefix + tenant_name
        create_kpi_query = "INSERT INTO `kpi`(`uuid`, `name`, `tenant_id`, `time_period`, `operation`, `timezone`) VALUES ('{uuid}','{name}',(SELECT id FROM tenant WHERE name='IOTSENS'),'DAILY','COUNT','Europe/Madrid')" \
            .format(uuid=kpi_uuid, name=kpi_name)
        cursor.execute(create_kpi_query)
        conn.commit()
        print("[TestKpis] Created KPI for tenant {tenant_name}, with name {kpi_name}.".format(tenant_name=tenant_name, kpi_name=kpi_name))
    else:
        print("[TestKpis] Found KPI for tenant {tenant_name}".format(tenant_name=tenant_name))
        kpi_uuid = fetch_result[0]

    ### MariaDB - Get sensors DF ###
    mariadb_sensors_query = """
        SELECT t.uuid as tenant_uuid, d.uuid as device_uuid, dm.uuid as measurement_uuid
        FROM device_measurement AS dm
        JOIN device AS d ON d.id=dm.device_id
        JOIN tenant as t ON d.tenant_id=t.id
        WHERE dm.date_last_value_reported IS NOT NULL
        AND t.name='{tenant_name}'
        """.format(tenant_name=tenant_name)
    mariadb_df = spark.read \
        .format("jdbc") \
        .option("url", mariadb_url) \
        .option("driver", "com.mysql.cj.jdbc.Driver") \
        .option("user", mariadb_user) \
        .option("password", mariadb_password) \
        .option("query", mariadb_sensors_query) \
        .load() \
        .repartition(24)
    mariadb_df_count = mariadb_df.rdd.cache().count()
    mariadb_end = time.time()

    ### Cassandra - Count sensor measures and insert results ###
    def cassandra_count_partition(iterator):
        profile = ExecutionProfile(consistency_level=ConsistencyLevel.LOCAL_QUORUM)
        cl = Cluster(cassandra_host, execution_profiles={EXEC_PROFILE_DEFAULT: profile})
        ssn = cl.connect('iotsens')
        lookup_stmt = ssn.prepare("SELECT count(*) FROM measurements_by_device_sensor_and_date WHERE tenant_id=? AND device_id=? AND sensor_id=? AND date=?")
        lookup_stmt.consistency_level = ConsistencyLevel.LOCAL_QUORUM
        for row in iterator:
            r = ssn.execute(lookup_stmt, [uuid.UUID(row.tenant_uuid), uuid.UUID(row.device_uuid), uuid.UUID(row.measurement_uuid), date]).one()
            yield r.count
        ssn.shutdown()
        cl.shutdown()

    start_date = datetime.strptime(start_date_str, "%Y-%m-%d")
    end_date = datetime.strptime(end_date_str, "%Y-%m-%d")
    current_date = start_date
    unix_timestamp = 0
    total_measures = 0
    total_measures_day = 0
    total_iterations = 0

    cluster = Cluster(cassandra_host)
    session = cluster.connect('***')
    while current_date <= end_date:

        # Parse date
        date = current_date.strftime("%Y-%m-%d")
        madrid_tz = pytz.timezone("Europe/Madrid")
        localized_date = madrid_tz.localize(current_date)
        unix_timestamp = int(localized_date.timestamp()) * 1000
        print("[TestKpis] Tenant", tenant_name, ", calculating day", date)

        # Count
        total_measures_day = mariadb_df.rdd.mapPartitions(cassandra_count_partition).sum()

        # Insert
        cassandra_query = "INSERT INTO kpis_by_date (tenant_id, kpi_id, date, timestamp, value) VALUES (f293d9cc-caf9-43c1-ba80-c0c93938c128,{kpi_id},'{date}',{timestamp},'{value}')" \
            .format(kpi_id=kpi_uuid, date=date, timestamp=unix_timestamp, value=total_measures_day)
        session.execute(cassandra_query)

        # Increase date and total
        total_measures += total_measures_day
        total_iterations += 1
        current_date += timedelta(days=1)

    session.shutdown()
    cluster.shutdown()
    cassandra_end = time.time()

    ### MariaDB - Update last value ###
    mariadb_get_query = "SELECT UNIX_TIMESTAMP(date_last_value_reported) AS timestamp FROM kpi WHERE uuid='{kpi_uuid}'".format(kpi_uuid=kpi_uuid)
    cursor.execute(mariadb_get_query)
    old_unix_timestamp = cursor.fetchone()
    if (old_unix_timestamp is None) or (old_unix_timestamp[0] is None) or ((old_unix_timestamp[0] * 1000) <= unix_timestamp):
        mariadb_update_query = "UPDATE kpi SET date_last_value_reported=FROM_UNIXTIME({timestamp}), last_value_reported='{value}' WHERE uuid='{kpi_uuid}'" \
            .format(timestamp=unix_timestamp / 1000, value=total_measures_day, kpi_uuid=kpi_uuid)
        cursor.execute(mariadb_update_query)
        conn.commit()
    mariadb_update_end = time.time()

    ### Data and time spent ###
    end = time.time()
    rate = (mariadb_df_count * total_iterations) / (end - start)
    print("[TestKpis]")
    print("[TestKpis] ######################### RESULTS #########################")
    print("[TestKpis]")
    print("[TestKpis] ---------- VARS ----------")
    print("[TestKpis] Start date      ", start_date.strftime("%Y-%m-%d"))
    print("[TestKpis] End date        ", end_date.strftime("%Y-%m-%d"))
    print("[TestKpis] Tenant          ", tenant_name)
    print("[TestKpis] KPI name        ", kpi_prefix + tenant_name)
    print("[TestKpis] KPI UUID        ", kpi_uuid)
    print("[TestKpis]")
    print("[TestKpis] ---------- DATA ----------")
    print("[TestKpis] Total sensors   ", mariadb_df_count)
    print("[TestKpis] Total measures  ", total_measures)
    print("[TestKpis]")
    print("[TestKpis] ---------- TIME ----------")
    print("[TestKpis] MariaDB         ", round(mariadb_end - start, 2), "s")
    print("[TestKpis] Cassandra       ", round(cassandra_end - mariadb_end, 2), "s")
    print("[TestKpis] MariaDB U       ", round(mariadb_update_end - cassandra_end, 2), "s")
    print("[TestKpis] Total           ", round(end - start, 2), "s")
    if rate > 0:
        print("[TestKpis] Rate            ", round(rate, 2), "sensors/s (" + str(round((1 / rate * 1000), 2)) + "ms per sensor)")
    print("[TestKpis]")
    print("[TestKpis] ###########################################################")
    print("[TestKpis]")

### Close connections ###
cursor.close()
conn.close()
