FROM spark:3.5.3-scala2.12-java17-python3-ubuntu
COPY --chown=2000:2000 script.py /opt/spark/work-dir/script.py
COPY --chown=2000:2000 chinook.db /opt/spark/work-dir/chinook.db
ENTRYPOINT ["/opt/spark/bin/spark-submit", "/opt/spark/work-dir/script.py"]