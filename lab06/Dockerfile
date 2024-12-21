# Используем базовый образ Ubuntu
FROM ubuntu:20.04

# Устанавливаем необходимые пакеты
RUN apt-get update && apt-get install -y wget openjdk-17-jdk python3

# Скачиваем Spark
RUN wget -O /opt/spark.tgz https://downloads.apache.org/spark/spark-3.5.3/spark-3.5.3-bin-hadoop3.tgz && \
    tar -xzf /opt/spark.tgz -C /opt/ && \
    mv /opt/spark-3.5.3-bin-hadoop3 /opt/spark && \
    rm /opt/spark.tgz

# Скачиваем JDBC драйвер для SQLite
RUN wget -O /opt/spark/work-dir/sqlite-jdbc-3.36.0.3.jar https://repo1.maven.org/maven2/org/xerial/sqlite-jdbc/3.36.0.3/sqlite-jdbc-3.36.0.3.jar

# Копируем скрипт и базу данных
COPY script.py /opt/spark/work-dir/script.py
COPY chinook.db /opt/spark/work-dir/chinook.db

# Устанавливаем рабочую директорию
WORKDIR /opt/spark/work-dir

# Запускаем скрипт
ENTRYPOINT ["/opt/spark/bin/spark-submit", "script.py"]