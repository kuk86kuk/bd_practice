from pyspark.sql import SparkSession
import sqlite3
import psycopg2

# Создаем SparkSession
spark = SparkSession.builder \
    .appName("SQLite to PostgreSQL") \
    .config("spark.jars", "/opt/spark/jars/sqlite-jdbc-3.47.1.0.jar,/opt/spark/jars/postgresql-42.7.3.jar") \
    .getOrCreate()

# Подключение к SQLite
sqlite_conn = sqlite3.connect('/app/chinook.db')
sqlite_cursor = sqlite_conn.cursor()

# Подключение к PostgreSQL
pg_conn = psycopg2.connect(
    dbname="mydatabase",
    user="myuser",
    password="mypassword",
    host="postgres",  # Имя сервиса PostgreSQL в Docker Compose
    port="5432"
)
pg_cursor = pg_conn.cursor()

# Функция для создания таблицы-источника в SQLite
def create_source_table():
    sqlite_cursor.execute('''
    CREATE TABLE IF NOT EXISTS source_table (
        id INTEGER PRIMARY KEY,
        name VARCHAR(100),
        created_at DATETIME
    )
    ''')
    sqlite_cursor.execute('''
    INSERT INTO source_table (id, name, created_at)
    VALUES (1, 'John Doe', '2023-10-01 10:00:00'),
           (2, 'Jane Smith', '2023-10-02 11:00:00')
    ''')
    sqlite_conn.commit()
    print("Таблица source_table создана и заполнена данными в SQLite.")

# Функция для получения информации о таблице из SQLite
def get_sqlite_table_info(table_name):
    sqlite_cursor.execute(f"PRAGMA table_info({table_name})")
    columns = sqlite_cursor.fetchall()
    return columns

# Функция для создания таблицы-приёмника в PostgreSQL
def create_pg_table(table_name, columns):
    column_defs = []
    for col in columns:
        col_name = col[1]
        col_type = col[2]
        if col_type == "INTEGER":
            pg_type = "INTEGER"
        elif col_type == "VARCHAR":
            pg_type = "VARCHAR(100)"
        elif col_type == "DATETIME":
            pg_type = "VARCHAR(100)"
        else:
            pg_type = "VARCHAR(100)"  # По умолчанию
        column_defs.append(f"{col_name} {pg_type}")
    
    create_table_query = f"CREATE TABLE IF NOT EXISTS {table_name} ({', '.join(column_defs)})"
    pg_cursor.execute(create_table_query)
    pg_conn.commit()
    print(f"Таблица {table_name} создана в PostgreSQL.")

# Функция для переноса данных из SQLite в PostgreSQL
def transfer_data(table_name):
    # Чтение данных из SQLite
    sqlite_cursor.execute(f"SELECT * FROM {table_name}")
    rows = sqlite_cursor.fetchall()
    
    # Вставка данных в PostgreSQL
    for row in rows:
        placeholders = ', '.join(['%s'] * len(row))
        insert_query = f"INSERT INTO {table_name} VALUES ({placeholders})"
        pg_cursor.execute(insert_query, row)
    
    pg_conn.commit()
    print(f"Данные из таблицы {table_name} перенесены в PostgreSQL.")

# Создаем таблицу-источник в SQLite
create_source_table()

# Список таблиц для переноса
tables_to_transfer = ["source_table"]

# Перенос данных
for table in tables_to_transfer:
    print(f"Обработка таблицы: {table}")
    
    # Получение информации о таблице
    table_info = get_sqlite_table_info(table)
    
    # Создание таблицы в PostgreSQL
    create_pg_table(table, table_info)
    
    # Перенос данных
    transfer_data(table)

# Закрытие соединений
sqlite_conn.close()
pg_conn.close()

# Останавливаем SparkSession
spark.stop()

print("Перенос данных завершен.")