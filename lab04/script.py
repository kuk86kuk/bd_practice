from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lag, mean, max
from pyspark.sql.window import Window



spark = SparkSession.builder \
    .appName("Lab04") \
    .getOrCreate()

df = spark.read.csv("/app/population.csv", header=True, inferSchema=True)

# Создаем окно для вычисления прироста населения от года к году
w = Window.partitionBy("Country Name").orderBy("Year")

# Вычисляем прирост населения от года к году и удаляем ненужные столбцы
df_increase = df \
    .withColumn('increase', col('Value') - lag('Value', 1).over(w)) \
    .drop('Country Code', 'Value')


# Фильтруем данные для периода с 1990 по 2018 год
df_trend = df_increase \
    .filter(col('Year').between(1990, 2018)) \
    .groupBy('Country Name') \
    .agg(mean('increase').alias('trend'))


# Создаем окно для вычисления максимального прироста
w1 = Window.partitionBy('Country Name')

# Фильтруем данные для периода с 1990 по 2018 год и вычисляем тренд для стран с отрицательным максимальным приростом
df_increase \
    .filter(col('Year').between(1990, 2018)) \
    .withColumn('max_inc', max('increase').over(w1)) \
    .filter(col('max_inc') < 0) \
    .groupBy('Country Name') \
    .agg(mean('increase').alias('trend')) \
    .show()

spark.stop()