from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, coalesce, first
from pyspark.sql.window import Window
from pyspark.sql.types import StructType, StructField, StringType, LongType

spark = SparkSession.builder.appName("Lab05").getOrCreate()

schema = StructType([
    StructField("id", StringType(), True),
    StructField("op", StringType(), True),
    StructField("ts", LongType(), True),
    StructField("data", StructType([
        StructField("account_id", StringType(), True),
        StructField("name", StringType(), True),
        StructField("address", StringType(), True),
        StructField("phone_number", StringType(), True),
        StructField("email", StringType(), True)
    ]), True),
    StructField("set", StructType([
        StructField("address", StringType(), True),
        StructField("phone_number", StringType(), True),
        StructField("savings_account_id", StringType(), True),
        StructField("card_id", StringType(), True),
        StructField("email", StringType(), True)
    ]), True)
])

df = spark.read.json("/app/accounts/*.json", schema=schema)

df = df \
    .withColumn('account_id', col('data.account_id')) \
    .withColumn('name', col('data.name')) \
    .withColumn('address', coalesce(col('data.address'), col('set.address'))) \
    .withColumn('phone_number', coalesce(col('data.phone_number'), col('set.phone_number'))) \
    .withColumn('email', coalesce(col('data.email'), col('set.email'))) \
    .withColumn('savings_account_id', col('set.savings_account_id')) \
    .withColumn('card_id', col('set.card_id')) \
    .drop('data', 'op', 'set')
df.show()

df \
.withColumn('account_id', first(col('account_id'), ignorenulls=True).over(Window.partitionBy('id').orderBy('ts'))) \
.withColumn('name', first(col('name'), ignorenulls=True).over(Window.partitionBy('id').orderBy('ts'))) \
.withColumn('address', first(col('address'), ignorenulls=True).over(Window.partitionBy('id').orderBy('ts'))) \
.withColumn('phone_number', first(col('phone_number'), ignorenulls=True).over(Window.partitionBy('id').orderBy('ts'))) \
.withColumn('email', first(col('email'), ignorenulls=True).over(Window.partitionBy('id').orderBy('ts'))) \
.withColumn('savings_account_id', first(col('savings_account_id'), ignorenulls=True).over(Window.partitionBy('id').orderBy('ts'))) \
.withColumn('address', col('address')) \
.withColumn('phone_number', col('phone_number')) \
.withColumn('email', col('email')) \
.orderBy('id', 'ts', 'account_id') \
.select('ts', 'account_id', 'address', 'email', 'name', 'phone_number', 'card_id', 'savings_account_id') \
.show()