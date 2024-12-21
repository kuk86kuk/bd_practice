# Используем базовый образ Apache Spark
FROM apache/spark:latest

# Устанавливаем Python и необходимые библиотеки
USER root
RUN apt-get update && apt-get install -y python3 python3-pip
RUN pip3 install pyspark psycopg2-binary

# Копируем драйверы JDBC
COPY sqlite-jdbc-3.47.1.0.jar /opt/spark/jars/sqlite-jdbc-3.47.1.0.jar
COPY postgresql-42.7.3.jar /opt/spark/jars/postgresql-42.7.3.jar

# Копируем Python-скрипт и SQLite базу данных
COPY spark_job.py /app/spark_job.py
COPY chinook.db /app/chinook.db

# Исправляем права доступа к SQLite базе данных
RUN chmod 644 /app/chinook.db

# Указываем рабочую директорию
WORKDIR /app

# Команда для запуска скрипта
CMD ["/opt/spark/bin/spark-submit", "/app/spark_job.py"]