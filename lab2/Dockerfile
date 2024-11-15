# Используем образ Spark с установленным Scala
FROM spark:3.5.3-scala2.12-java17-python3-ubuntu

# Копируем исходный код Scala и данные в контейнер
COPY --chown=2000:2000 src/main/scala/First5Records.scala /opt/spark/work-dir/First5Records.scala
COPY --chown=2000:2000 ml-100k/u.data /opt/spark/work-dir/data/u.data

# Компилируем Scala-код
RUN scalac -classpath "/opt/spark/jars/*" /opt/spark/work-dir/First5Records.scala -d /opt/spark/work-dir/

# Запускаем Scala-код с помощью spark-submit
ENTRYPOINT ["/opt/spark/bin/spark-submit", "--class", "First5Records", "/opt/spark/work-dir/First5Records.jar"]