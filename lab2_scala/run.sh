#!/bin/bash

# Распаковываем архив ml-100k.zip в папку data
unzip data/ml-100k.zip -d data

# Собираем Docker-образ с именем lab02
docker build -t lab02 .

# Запускаем Docker-контейнер и перенаправляем вывод в файл out.txt
docker run lab02 > out.txt