import os
import datetime
import sqlite3
import re
import pandas as pd
from tqdm import tqdm
import csv
import json

class DatabaseManager:
    """
    A class to manage database operations.
    """
    def __init__(self, db_path):
        self.db_path = db_path
        self.conn = sqlite3.connect(db_path)
        self.create_table()

    def create_table(self):
        """
        Create a table named 'search_phrases' if it doesn't exist.
        """
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
        """
        Insert phrases into the database.

        Args:
            phrases (list): List of phrases to insert.
        """
        with tqdm(total=len(phrases), desc="Adding phrases to the database") as pbar:
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
        """
        Remove phrases containing negative words from the database.

        Args:
            negative_words (list): List of negative words to check and remove.
        """
        with tqdm(total=len(negative_words), desc="Removing negative phrases") as pbar:
            cursor = self.conn.cursor()
            for word in negative_words:
                cursor.execute('DELETE FROM search_phrases WHERE phrase LIKE ?', ('%' + word.strip() + '%',))
                pbar.update(1)
                self.conn.commit()

    def export_to_csv(self, start_date, end_date):
        """
        Export data from the database to a CSV file within the specified date range.

        Args:
            start_date (str): Start date in the format dd-mm-yyyy.
            end_date (str): End date in the format dd-mm-yyyy.

        Returns:
            list: Data from the database within the date range.
        """
        cursor = self.conn.cursor()
        cursor.execute('SELECT * FROM search_phrases WHERE date BETWEEN ? AND ?', (start_date, end_date))
        data = cursor.fetchall()
        return data

    def close_connection(self):
        """
        Close the database connection.
        """
        self.conn.close()

class FileManager:
    """
    A class to manage file-related operations.
    """
    @staticmethod
    def load_config():
        """
        Load configuration from config.json file.

        Returns:
            dict: Configuration data.
        """
        config_path = os.path.join(os.path.dirname(__file__), 'config.json')
        with open(config_path, 'r') as config_file:
            config = json.load(config_file)
        return config

    @staticmethod
    def process_search_phrases_file(file_path):
        """
        Process the CSV file containing search phrases and clean them.

        Args:
            file_path (str): Path to the CSV file.

        Returns:
            list: List of cleaned search phrases.
        """
        try:
            phrases = []
            with open(file_path, 'r', encoding='utf-8') as csv_file:
                csv_reader = csv.reader(csv_file)
                for line in tqdm(csv_reader, desc="Processing search phrases"):
                    for phrase in line:
                        cleaned_phrase = FileManager.clean_text(phrase.strip())
                        if cleaned_phrase:
                            phrases.append(cleaned_phrase)
            return phrases
        except FileNotFoundError:
            raise Exception("CSV file not found")
        except Exception as e:
            raise e

    @staticmethod
    def process_negative_words_file(csv_file_path):
        """
        Process the CSV file containing negative words.

        Args:
            csv_file_path (str): Path to the CSV file.

        Returns:
            list: List of cleaned negative words.
        """
        try:
            negative_words = []
            with open(csv_file_path, 'r', encoding='utf-8') as csv_file:
                csv_reader = csv.reader(csv_file, delimiter=';')
                for line in tqdm(csv_reader, desc="Processing negative words"):
                    negative_words.extend([word.strip() for word in line if word.strip()])
            return negative_words
        except FileNotFoundError:
            raise Exception("CSV file not found")
        except Exception as e:
            raise e

    @staticmethod
    def clean_text(text):
        """
        Clean text by removing extra spaces and special characters.

        Args:
            text (str): Input text to be cleaned.

        Returns:
            str: Cleaned text.
        """
        cleaned_text = re.sub(r'\s+', ' ', text).strip()
        cleaned_text = re.sub(r'[^\w\s]', '', cleaned_text)
        return cleaned_text

class MenuManager:
    """
    A class to manage menu-related operations.
    """
    @staticmethod
    def print_menu():
        """
        Display the main menu options.
        """
        print("Select operation:")
        print("a. Export existing database file for a period to CSV")
        print("b. Add new search phrases")
        print("c. Exit")

    @staticmethod
    def get_user_choice():
        """
        Get the user's choice for the operation.

        Returns:
            str: User's choice (a/b/c).
        """
        return input("Enter the operation letter (a/b/c): ").lower()

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

            if choice == 'a':
                start_date = input("Enter the start date in dd-mm-yyyy format: ")
                end_date = input("Enter the end date in dd-mm-yyyy format: ")
                try:
                    data = db_manager.export_to_csv(start_date, end_date)
                    if data:
                        df = pd.DataFrame(data, columns=['id', 'phrase', 'date'])
                        export_path = os.path.join(os.path.dirname(__file__), f'export_{start_date}_{end_date}.csv')
                        df.to_csv(export_path, index=False)
                        print(f"Data exported to file: {export_path}")
                    else:
                        print("No data to export for the specified period.")
                    log_entry = f"{datetime.datetime.now()}: Data export for the period {start_date} - {end_date} successful."
                    with open(log_file_path, 'a', encoding='utf-8') as log_file:
                        log_file.write(log_entry + '\n')
                except Exception as e:
                    log_entry = f"{datetime.datetime.now()}: Error exporting data for the period {start_date} - {end_date}: {e}"
                    with open(log_file_path, 'a', encoding='utf-8') as log_file:
                        log_file.write(log_entry + '\n')
            elif choice == 'b':
                try:
                    file_path = input("Enter the path to the CSV file with search phrases: ")
                    phrases = FileManager.process_search_phrases_file(file_path)
                    db_manager.insert_phrases(phrases)
                    csv_file_path = input("Enter the path to the CSV file with negative words: ")
                    negative_words = FileManager.process_negative_words_file(csv_file_path)
                    db_manager.remove_phrases(negative_words)
                    log_entry = f"{datetime.datetime.now()}: Processing new phrases and removing negative phrases completed."
                    with open(log_file_path, 'a', encoding='utf-8') as log_file:
                        log_file.write(log_entry + '\n')
                except Exception as e:
                    log_entry = f"{datetime.datetime.now()}: Error processing new phrases and removing negative phrases: {e}"
                    with open(log_file_path, 'a', encoding='utf-8') as log_file:
                        log_file.write(log_entry + '\n')
            elif choice == 'c':
                break
            else:
                print("Invalid operation choice. Please select a/b/c.")

        print("Program terminated.")
    
    except Exception as e:
        print("An error occurred:", e)
    finally:
        db_manager.close_connection()

if __name__ == "__main__":
    main()
