
import sqlite3

def drop_and_recreate_tables(db_path='school.db'):
    conn = sqlite3.connect(db_path)
    c = conn.cursor()

    # Drop old tables
    c.execute("DROP TABLE IF EXISTS fees;")
    c.execute("DROP TABLE IF EXISTS bursaries;")

    # Recreate fees table
    c.execute('''
        CREATE TABLE fees (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            student_id INTEGER,
            term TEXT,
            year INTEGER,
            expected_amount REAL,
            bursary_amount REAL DEFAULT 0,
            carried_forward REAL DEFAULT 0,
            amount_paid REAL DEFAULT 0,
            date_paid TEXT,
            method TEXT DEFAULT 'N/A',
            payment_type TEXT DEFAULT 'school_fees',
            FOREIGN KEY(student_id) REFERENCES students(id)
        )
    ''')

    # Recreate bursaries table
    c.execute('''
        CREATE TABLE bursaries (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            student_id INTEGER,
            sponsor_name TEXT,
            term TEXT,
            year INTEGER,
            amount REAL,
            FOREIGN KEY(student_id) REFERENCES students(id)
        )
    ''')

    conn.commit()
    conn.close()
    print("Tables 'fees' and 'bursaries' dropped and recreated successfully.")

# Run this file manually to reset tables
if __name__ == '__main__':
    drop_and_recreate_tables()
