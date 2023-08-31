import os
import sqlite3
import pandas as pd
from tqdm import tqdm

class DatabaseCheck:
    def __init__(self):
        self.database_path = "portals_traffic.db"
        self.conn = sqlite3.connect(self.database_path)
        self.cursor = self.conn.cursor()

        self.create_log_entry("Database created")

    def create_log_entry(self, entry):
        log_path = "logs"
        os.makedirs(log_path, exist_ok=True)
        log_file = os.path.join(log_path, "log.txt")

        with open(log_file, "a") as f:
            f.write(f"{entry} at {pd.Timestamp.now()}\n")

    def check_indexes_table(self):
        self.cursor.execute("CREATE TABLE IF NOT EXISTS indexes (url TEXT PRIMARY KEY)")
        self.create_log_entry("Indexes table created")

    def check_data_table(self, table_name):
        self.cursor.execute(f"CREATE TABLE IF NOT EXISTS {table_name} (url TEXT PRIMARY KEY, Google TEXT, Yandex TEXT, SeansAll TEXT)")
        self.create_log_entry(f"Data table {table_name} created")

        self.cursor.execute(f"SELECT url FROM {table_name}")
        existing_urls = set(row[0] for row in self.cursor.fetchall())
        return existing_urls

    def close_connection(self):
        self.conn.commit()
        self.conn.close()

class DatabaseUpdater:
    def __init__(self, csv_file):
        self.csv_file = csv_file
        self.table_name = os.path.splitext(os.path.basename(csv_file))[0]
        self.df = pd.read_csv(csv_file, encoding="utf-8")
        self.db_check = DatabaseCheck()

    def update_database(self):
        existing_urls = self.db_check.check_data_table(self.table_name)
        urls_to_add = []

        db_columns = [col[1] for col in self.db_check.cursor.execute(f"PRAGMA table_info({self.table_name})")]

        for index, row in tqdm(self.df.iterrows(), total=len(self.df), desc=f"Processing {self.table_name}"):
            url = row["URL"]
            if url not in existing_urls:
                urls_to_add.append(url)

        if urls_to_add:
            self.db_check.cursor.executemany(f"INSERT INTO {self.table_name} (url) VALUES (?)", [(url,) for url in urls_to_add])
            self.db_check.create_log_entry(f"Added {len(urls_to_add)} new URLs to {self.table_name} table")

        self.db_check.conn.commit()

        for index, row in tqdm(self.df.iterrows(), total=len(self.df), desc=f"Updating {self.table_name}"):
            url = row["URL"]
            if url in existing_urls:
                update_data = tuple(row[col] if col in self.df.columns else None for col in db_columns[1:])  # Exclude the "url" column
                self.db_check.cursor.execute(f"UPDATE {self.table_name} SET Google=?, Yandex=?, SeansAll=? WHERE url=?", 
                                             update_data + (url,))

        self.db_check.conn.commit()
        self.db_check.close_connection()

class DatabaseExporter:
    def export_to_csv(self):
        db_check = DatabaseCheck()
        tables = ["zakupka", "satom", "tomasBy", "tomasKz"]
        export_data = {}

        for table in tables:
            db_check.cursor.execute(f"SELECT * FROM {table}")
            rows = db_check.cursor.fetchall()
            for row in rows:
                url = row[0]
                data = row[1:]
                if url not in export_data:
                    export_data[url] = {}
                export_data[url][table] = data

        reformatted_data = []
        for url, table_data in export_data.items():
            row = [url]
            for table in tables:
                if table in table_data:
                    row.extend(table_data[table])
                else:
                    row.extend(["0", "0", "0"])  # If data is missing, fill with zeros
            reformatted_data.append(row)

        header = ["URL"]
        for table in tables:
            header.extend([f"{table}_Google", f"{table}_Yandex", f"{table}_SeansAll"])

        export_df = pd.DataFrame(reformatted_data, columns=header)
        export_path = os.path.join(os.path.dirname(__file__), "exported_data.csv")  # Save in the same directory as the script
        export_df.to_csv(export_path, encoding="utf-8", index=False)
        db_check.create_log_entry("Exported data to CSV")

        db_check.close_connection()

def main():
    db_check = DatabaseCheck()
    db_check.cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='indexes'")
    if db_check.cursor.fetchone() is None:
        db_check.check_indexes_table()

    while True:
        choice = input("Choose operation:\n1. Update database\n2. Export data to CSV\n3. Exit\n")

        if choice == "1":
            csv_files = ["zakupka", "satom", "tomasBy", "tomasKz"]
            for idx, csv_file in enumerate(csv_files, start=1):
                print(f"{idx}. {csv_file}.csv")
            
            while True:
                file_choice = input("Enter the number of the file you want to update (1-4): ")
                if file_choice.isdigit() and 1 <= int(file_choice) <= len(csv_files):
                    chosen_csv_file = csv_files[int(file_choice) - 1]
                    file_path = input(f"Enter the path to '{chosen_csv_file}.csv': ")
                    if os.path.exists(file_path):
                        updater = DatabaseUpdater(file_path)
                        updater.update_database()
                        break
                    else:
                        print(f"CSV file '{file_path}' not found.")
                        continue
                else:
                    print("Invalid choice. Please enter a valid number.")
            
            print("Database updated successfully.")
        elif choice == "2":
            exporter = DatabaseExporter()
            exporter.export_to_csv()
            print("Data exported to CSV successfully.")
        elif choice == "3":
            db_check.close_connection()
            break
        else:
            print("Invalid choice. Please select again.")

if __name__ == "__main__":
    main()
