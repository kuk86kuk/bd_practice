#!/bin/bash

# Проверка наличия аргументов
if [ "$#" -eq 0 ]; then
    echo "Usage: $0 file1.txt file2.txt ..."
    exit 1
fi

echo "Запускаю word_count.py"
python word_count.py "$@"
echo "Работа word_count.py завершена"