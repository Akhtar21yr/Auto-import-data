import time
import os
import sqlite3
import pandas as pd
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
import tempfile  # To create temporary files

# Step 1: CSV to Database Function
def csv_to_db(file_path):
    try:
        conn = sqlite3.connect('ld_data.db')
        cursor = conn.cursor()
        
        # Create the STUDENTS table if it doesn't exist
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS STUDENTS (
                name TEXT,
                roll INTEGER,
                city TEXT,
                age INTEGER,
                gender TEXT,
                state TEXT
            )
            """
        )
        
        # Read CSV into DataFrame
        df = pd.read_csv(file_path, encoding='utf-8')  # Ensure consistent format
        # Ensure consistent column order
        expected_columns = ['name', 'roll', 'city', 'age', 'gender', 'state']
        if set(df.columns) != set(expected_columns):
            raise ValueError("CSV column names do not match expected names")

        # Insert CSV data into the table
        for _, row in df[expected_columns].iterrows():  # Preserve column order
            cursor.execute(
                "INSERT INTO STUDENTS (name, roll, city, age, gender, state) VALUES (?, ?, ?, ?, ?, ?)",
                tuple(row)
            )

        conn.commit()
        print("Data inserted successfully.")

        # Step 2: Export to CSV after insertion with explicit column order
    except Exception as e:
        print(f"Error during database operations: {e}")
    finally:
        if 'cursor' in locals():
            cursor.close()
        if 'conn' in locals():
            conn.close()

# Step 3: Debounced File Event Handler
class DebouncedFileEventHandler(FileSystemEventHandler):
    def __init__(self, file_path, debounce_interval=2.0):  # Longer debounce interval
        super().__init__()
        self.file_path = file_path
        self.debounce_interval = debounce_interval
        self.last_modified_time = 0

    def on_modified(self, event):
        current_time = time.time()
        if event.src_path == self.file_path and (current_time - self.last_modified_time) > self.debounce_interval:
            self.last_modified_time = current_time
            csv_to_db(self.file_path)

# Step 4: Setup Observer
file_to_watch = r"C:\Users\admin\Desktop\auto_export_to_db\student.csv"

# Ensure the CSV file exists
if not os.path.exists(file_to_watch):
    raise FileNotFoundError(f"The specified file does not exist: {file_to_watch}")

# Initialize Watchdog Observer
observer = Observer()
event_handler = DebouncedFileEventHandler(file_to_watch)

# Schedule observer
observer.schedule(event_handler, path=os.path.dirname(file_to_watch), recursive=False)

# Start observer
observer.start()

try:
    while True:
        time.sleep(1)  # Regular check intervals
except KeyboardInterrupt:
    observer.stop()
except Exception as e:
    print(f"An error occurred: {e}")

# Wait until observer stops
observer.join()
