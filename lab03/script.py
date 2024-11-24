from pyspark.sql import SparkSession
from pyspark.sql.functions import count, countDistinct, desc

# Создаем сессию Spark
spark = SparkSession.builder \
    .appName("Lab03") \
    .getOrCreate()

# Загружаем данные из CSV файла
df = spark.read.csv("/app/world-cities.csv", header=True, inferSchema=True)

# Выводим схему кадра данных
df.printSchema()

# Группируем данные по стране, считаем количество уникальных регионов и общее количество городов
result = df.groupBy("country") \
    .agg(countDistinct("subcountry").alias("subcountry"), count("name").alias("cnt")) \
    .orderBy(desc("cnt"))

# Выводим результат
result.show(20)

# Останавливаем сессию Spark
spark.stop()