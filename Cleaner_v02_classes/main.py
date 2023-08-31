import os
import datetime
import sqlite3
import re
import pandas as pd
from tqdm import tqdm
import csv
import json

class DatabaseManager:
    def __init__(self, db_path):
        self.db_path = db_path
        self.conn = sqlite3.connect(db_path)
        self.create_table()

    def create_table(self):
        cursor = self.conn.cursor()
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS search_phrases (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            phrase TEXT,
            date DATE
        )
        ''')
        self.conn.commit()

    def insert_phrases(self, phrases):
        with tqdm(total=len(phrases), desc="Добавление фраз в базу данных") as pbar:
            cursor = self.conn.cursor()
            current_date = datetime.datetime.now().strftime('%d-%m-%Y')
            for phrase in phrases:
                cursor.execute('SELECT COUNT(*) FROM search_phrases WHERE phrase = ?', (phrase,))
                duplicate_count = cursor.fetchone()[0]
                if duplicate_count == 0:
                    cursor.execute('INSERT INTO search_phrases (phrase, date) VALUES (?, ?)', (phrase, current_date))
                pbar.update(1)
                self.conn.commit()

    def remove_phrases(self, negative_words):
        with tqdm(total=len(negative_words), desc="Удаление минус-фраз") as pbar:
            cursor = self.conn.cursor()
            for word in negative_words:
                cursor.execute('DELETE FROM search_phrases WHERE phrase LIKE ?', ('%' + word.strip() + '%',))
                pbar.update(1)
                self.conn.commit()

    def export_to_csv(self, start_date, end_date):
        cursor = self.conn.cursor()
        cursor.execute('SELECT * FROM search_phrases WHERE date BETWEEN ? AND ?', (start_date, end_date))
        data = cursor.fetchall()
        return data

    def close_connection(self):
        self.conn.close()

class FileManager:
    @staticmethod
    def load_config():
        config_path = os.path.join(os.path.dirname(__file__), 'config.json')
        with open(config_path, 'r') as config_file:
            config = json.load(config_file)
        return config

    @staticmethod
    def process_search_phrases_file(file_path):
        try:
            phrases = []
            with open(file_path, 'r', encoding='utf-8') as csv_file:
                csv_reader = csv.reader(csv_file)
                for line in tqdm(csv_reader, desc="Обработка поисковых фраз"):
                    for phrase in line:
                        cleaned_phrase = FileManager.clean_text(phrase.strip())
                        if cleaned_phrase:
                            phrases.append(cleaned_phrase)
            return phrases
        except FileNotFoundError:
            raise Exception("CSV файл не найден")
        except Exception as e:
            raise e

    @staticmethod
    def process_negative_words_file(csv_file_path):
        try:
            negative_words = []
            with open(csv_file_path, 'r', encoding='utf-8') as csv_file:
                csv_reader = csv.reader(csv_file, delimiter=';')
                for line in tqdm(csv_reader, desc="Обработка минус-слов"):
                    negative_words.extend([word.strip() for word in line if word.strip()])
            return negative_words
        except FileNotFoundError:
            raise Exception("CSV файл не найден")
        except Exception as e:
            raise e

    @staticmethod
    def clean_text(text):
        cleaned_text = re.sub(r'\s+', ' ', text).strip()
        cleaned_text = re.sub(r'[^\w\s]', '', cleaned_text)
        return cleaned_text

class MenuManager:
    @staticmethod
    def print_menu():
        print("Выберите операцию:")
        print("а. Экспорт существующего файла базы данных за период в csv")
        print("б. Добавление новых поисковых фраз")
        print("в. Выход")

    @staticmethod
    def get_user_choice():
        return input("Введите букву операции (а/б/в): ").lower()

def main():
    try:
        config = FileManager.load_config()
        log_folder = os.path.join(os.path.dirname(__file__), 'logs')
        os.makedirs(log_folder, exist_ok=True)
        log_filename = config["log"]["filename"]
        log_file_path = os.path.join(log_folder, log_filename)
        
        db_filename = config["database"]["filename"]
        db_manager = DatabaseManager(os.path.join(os.path.dirname(__file__), db_filename))

        while True:
            MenuManager.print_menu()
            choice = MenuManager.get_user_choice()

            if choice == 'а':
                start_date = input("Введите начальную дату в формате dd-mm-yyyy: ")
                end_date = input("Введите конечную дату в формате dd-mm-yyyy: ")
                try:
                    data = db_manager.export_to_csv(start_date, end_date)
                    if data:
                        df = pd.DataFrame(data, columns=['id', 'phrase', 'date'])
                        export_path = os.path.join(os.path.dirname(__file__), f'export_{start_date}_{end_date}.csv')
                        df.to_csv(export_path, index=False)
                        print(f"Экспорт данных выполнен в файл: {export_path}")
                    else:
                        print("Нет данных для экспорта в указанный период.")
                    log_entry = f"{datetime.datetime.now()}: Выгрузка данных за период {start_date} - {end_date} выполнена успешно."
                    with open(log_file_path, 'a', encoding='utf-8') as log_file:
                        log_file.write(log_entry + '\n')
                except Exception as e:
                    log_entry = f"{datetime.datetime.now()}: Ошибка при выгрузке данных за период {start_date} - {end_date}: {e}"
                    with open(log_file_path, 'a', encoding='utf-8') as log_file:
                        log_file.write(log_entry + '\n')
            elif choice == 'б':
                try:
                    file_path = input("Введите путь к CSV файлу с поисковыми фразами: ")
                    phrases = FileManager.process_search_phrases_file(file_path)
                    db_manager.insert_phrases(phrases)
                    csv_file_path = input("Введите путь к CSV файлу с минус-словами: ")
                    negative_words = FileManager.process_negative_words_file(csv_file_path)
                    db_manager.remove_phrases(negative_words)
                    log_entry = f"{datetime.datetime.now()}: Обработка новых фраз и удаление минус-фраз выполнено."
                    with open(log_file_path, 'a', encoding='utf-8') as log_file:
                        log_file.write(log_entry + '\n')
                except Exception as e:
                    log_entry = f"{datetime.datetime.now()}: Ошибка при обработке новых фраз и удалении минус-фраз: {e}"
                    with open(log_file_path, 'a', encoding='utf-8') as log_file:
                        log_file.write(log_entry + '\n')
            elif choice == 'в':
                break
            else:
                print("Некорректный выбор операции. Пожалуйста, выберите а/б/в.")

        print("Программа завершена.")
    
    except Exception as e:
        print("Произошла ошибка:", e)
    finally:
        db_manager.close_connection()

if __name__ == "__main__":
    main()

