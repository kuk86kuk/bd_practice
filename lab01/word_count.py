import re, os
from collections import Counter

'''

1. __init__: Инициализация класса с передачей путей к файлам.

2. map: Чтение файла, преобразование текста, удаление знаков пунктуации, приведение к нижнему регистру и разбиение на слова.

3. combiner: Подсчет слов и фильтрация по длине (не менее 4 символов).

4. reduce: Объединение результатов из нескольких файлов, фильтрация по частоте (не менее 10 раз) и сортировка по убыванию частоты.

5. save_to_file: Сохранение результатов в файл result.txt.

6. process: Основной процесс обработки файлов, включающий фазы map, combiner, reduce и сохранение результатов.

'''

class WordCounter:
    def __init__(self, file_paths):
        self.file_paths = file_paths


    def map(self, file_path):
        with open(file_path, 'r', encoding='cp1251') as file:
            text = file.read()
        text = re.sub(r'[^\w\s]', '', text).lower()
        words = text.split()
        print(f'Количество слов в файле {file_path}: {len(words)}')
        print("Фаза map завершена")
        return words


    def combiner(self, words):
        word_counts = Counter(words)
        filtered_words = {word: count for word, count in word_counts.items() if len(word) >= 4}
        print("Фаза combiner завершена")
        return filtered_words


    def reduce(self, word_counts_list):
        combined_word_counts = Counter()
        for word_counts in word_counts_list:
            combined_word_counts.update(word_counts)
        filtered_words = {word: count for word, count in combined_word_counts.items() if count >= 10}
        sorted_word_counts = sorted(filtered_words.items(), key=lambda x: (-x[1], x[0]))
        print("Фаза reduce завершена")
        return sorted_word_counts


    def save_to_file(self, word_counts):
        if os.path.exists("result.txt"):
            print(f"Файл result.txt уже существует и будет перезаписан.")
        with open("result.txt", 'w', encoding='utf-8') as out_file:
            for word, count in word_counts:
                out_file.write(f"{word} - {count}\n")
        print(f"Результат сохранен в файл result.txt")


    def process(self):
        print("Начинаю обработку файлов...")
        all_word_counts = []
        for file_path in self.file_paths:
            words = self.map(file_path)
            word_counts = self.combiner(words)
            all_word_counts.append(word_counts)
        result = self.reduce(all_word_counts)
        self.save_to_file(result)
        print("10 самых часто встречающихся слов.")
        for word, count in result[:10]:
            print(f"{word} - {count}")
        print("Обработка файлов завершена.")



if __name__ == "__main__":
    import sys
    file_paths = sys.argv[1:]
    counter = WordCounter(file_paths)
    counter.process()
