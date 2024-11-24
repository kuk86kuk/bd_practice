FROM apache/spark:latest
WORKDIR /app
COPY script.py /app/script.py
COPY population.csv /app/population.csv
CMD ["/opt/spark/bin/spark-submit", "/app/script.py"]