import sqlite3

# Function to create SQLite database and table
def create_database_table():
    try:
        conn = sqlite3.connect('ld_data.db')
        cursor = conn.cursor()
        cursor.execute('''
            CREATE TABLE IF NOT EXISTS Students (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT,
                roll TEXT,
                city TEXT,
                age INTEGER,
                gender TEXT,
                state TEXT
            )
        ''')
        conn.commit()
    except Exception as e:
        print(f"Error creating database and table: {e}")
    finally:
        if 'cursor' in locals():
            cursor.close()
        if 'conn' in locals():
            conn.close()

create_database_table()
