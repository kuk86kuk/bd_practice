FROM apache/spark:latest
WORKDIR /app
COPY script.py /app/script.py
COPY accounts/*.json /app/accounts/
CMD ["/opt/spark/bin/spark-submit", "/app/script.py"]