from pyspark.sql import SparkSession
from sqlalchemy import create_engine, text
import pandas as pd

# Создание SparkSession
spark = SparkSession.builder \
    .appName("SQLite to PostgreSQL") \
    .config("spark.jars", "/opt/spark/jars/postgresql-42.2.5.jar") \
    .getOrCreate()

# Подключение к SQLite и PostgreSQL
sqlite = create_engine("sqlite:////opt/spark/work-dir/chinook.db")  # Используйте абсолютный путь
postgresql = create_engine("postgresql://me:mypass@postgres-source/mydatabase")  # Используйте имя сервиса

jdbcDriverLite = 'org.sqlite.JDBC'
jdbcUrlLite = 'jdbc:sqlite:/opt/spark/work-dir/chinook.db'  # Используйте абсолютный путь
jdbcUrlPost = 'jdbc:postgresql://postgres-source/mydatabase?user=me&password=mypass'  # Используйте имя сервиса
jdbcDriverPost = 'org.postgresql.Driver'

with sqlite.connect() as lite, lite.begin(), postgresql.connect() as post_conn, post_conn.begin():

    table_query = text('''SELECT name, sql FROM sqlite_master
                          WHERE type ='table' AND name NOT LIKE 'sqlite_%';''')

    tables = lite.execute(table_query)

    for row in tables:
        tab = row['name']
        field = []
        col = []

        columns_query = text(f'''SELECT name, type FROM pragma_table_info('{tab}') c''')
        inner_result = lite.execute(columns_query)

        for inner_row in inner_result:
            field_name = inner_row['name']
            field_type = str(inner_row['type']).replace('NVARCHAR', 'VARCHAR').replace('DATETIME', 'VARCHAR(100)')
            field.append(f"{field_name} {field_type}")

            col_expr = f"cast({field_name} as text)" if inner_row['type'] == 'DATETIME' else field_name
            col.append(col_expr)

        ddl = f"CREATE TABLE IF NOT EXISTS {tab} (\n" + ",\n".join(field) + ");"
        post_conn.execute(ddl)

        subquery = f'(SELECT {", ".join(col)} FROM {tab}) AS {tab}'

        df = spark.read.format('jdbc').options(driver=jdbcDriverLite, dbtable=subquery, url=jdbcUrlLite).load()
        df.show()
        df.write.format("jdbc").option("url", jdbcUrlPost).option("driver", jdbcDriverPost).option("dbtable", tab).mode("overwrite").save()

# Проверка созданных таблиц и количества строк в PostgreSQL
with postgresql.connect() as post, post.begin():
    cursor = post.execute('''SELECT * FROM pg_catalog.pg_tables where schemaname = 'public';''')
    for row in cursor:
        print(row['schemaname'] + '.' + row['tablename'])
        innerCursor = post.execute(f'''SELECT count(*) cnt FROM {row['schemaname']}.{row['tablename']};''')
        for innerRow in innerCursor:
            print(f'''Rows count: {innerRow['cnt']}''')

# Остановка SparkSession
spark.stop()