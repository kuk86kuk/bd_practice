import sqlite3

# Подключение к SQLite
conn = sqlite3.connect('chinook.db')
cursor = conn.cursor()

# Создание таблицы source_table
cursor.execute('''
CREATE TABLE IF NOT EXISTS source_table (
    id INTEGER PRIMARY KEY,
    name VARCHAR(100),
    created_at DATETIME
)
''')

# Вставка данных в таблицу
cursor.execute('''
INSERT INTO source_table (id, name, created_at)
VALUES (1, 'John Doe', '2023-10-01 10:00:00'),
       (2, 'Jane Smith', '2023-10-02 11:00:00')
''')

# Сохранение изменений
conn.commit()

# Закрытие соединения
conn.close()

print("Таблица source_table создана и заполнена данными.")