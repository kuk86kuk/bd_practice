FROM apache/spark:latest
WORKDIR /app
COPY script.py /app/script.py
COPY world-cities.csv /app/world-cities.csv
CMD ["/opt/spark/bin/spark-submit", "/app/script.py"]