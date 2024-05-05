import time
import os
import sqlite3
import pandas as pd
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
# import pyodbc  # For ODBC connections

# Connection string for SQL Server
CONNECTION_STRING = (
    "DRIVER=ODBC Driver 17 for SQL Server;"
    "SERVER=192.168.15.55;"
    "DATABASE=LD;"
    "UID=sa;"
    "PWD=Admin@4321;"
)

# Step 1: CSV to Database Function
def csv_to_db(file_path):
    try:
        # Connect to SQL Server using ODBC
        conn = conn = sqlite3.connect('ld_data.db')
        cursor = conn.cursor()
        cursor.execute('DELETE from LD')
        conn.commit()
        
        # Read CSV into DataFrame
        df = pd.read_csv(file_path, encoding='utf-8')  # Handle tab-separated data

        # Insert CSV data into the database
        for _, row in df.iterrows():
            # Use explicit column names to avoid mismatches
            print('-->>>>>>>>>>>>>>>>>>>>>>>>>..',row)
            cursor.execute(
                "INSERT INTO LD (date, group_name, Tran_Type, rec_and_paid, PARTICULARS, mtm_margin, rate, aed, inr, inr_aed) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                tuple(row)
            )

        conn.commit()
        print("Data inserted successfully into SQL Server.")

    except Exception as e:
        print(f"Error during data insertion: {e}")
    finally:
        if 'cursor' in locals():
            cursor.close()
        if 'conn' in locals():
            conn.close()

# Step 2: Debounced File Event Handler
class DebouncedFileEventHandler(FileSystemEventHandler):
    def __init__(self, file_path, debounce_interval=1.0):  # Longer debounce interval
        super().__init__()
        self.file_path = file_path
        self.debounce_interval = debounce_interval
        self.last_modified_time = 0

    def on_modified(self, event):
        current_time = time.time()
        if event.src_path == self.file_path and (current_time - self.last_modified_time) > self.debounce_interval:
            self.last_modified_time = current_time
            csv_to_db(self.file_path)

# Step 3: Setup Observer
file_to_watch = r"C:\Users\admin\Desktop\auto_export_to_db\LD.csv"

# Ensure the CSV file exists
if not os.path.exists(file_to_watch):
    raise FileNotFoundError(f"The specified file does not exist: {file_to_watch}")

observer = Observer()
event_handler = DebouncedFileEventHandler(file_to_watch)

observer.schedule(event_handler, path=os.path.dirname(file_to_watch), recursive=False)

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
