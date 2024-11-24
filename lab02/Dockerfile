FROM apache/spark:latest
COPY script.py /app/script.py
COPY u.data /app/u.data
ENTRYPOINT ["/opt/spark/bin/spark-submit", "/app/script.py"]