# Lab 06
# Работа с источниками данных в Apache Spark
Изучите теоретическую часть.

Ячейки ниже скачивают и настраивают две СУБД.

Задача состоит в том, что бы создать в postgresql таблицы приёмники (target table) и заполнить данные в этих таблицах из таблиц источников (source table) в sqlite.

**Подсказки:**
1. При создании таблиц вместо [типа](https://www.sqlite.org/datatype3.html) `NVARCHAR` используйте [тип](https://postgrespro.ru/docs/postgrespro/10/datatype) `VARCHAR`, а вместо типа `DATETIME` используйте тип `VARCHAR(100)`.
2. Если вам потребуется преобразовывать типы данных в sqlite это делается вот [так](https://www.sqlite.org/lang_expr.html#castexpr).
3. Для вывода списка таблиц предлагается использовать для [sqlite](https://www.sqlite.org/schematab.html) и для [postgresql](https://www.postgresql.org/docs/8.0/view-pg-tables.html)
4. В sqlite вы можете воспользоваться конструкцией `SELECT * FROM pragma_table_info(table_name)` для вывода типов и названий атрибутов таблицы