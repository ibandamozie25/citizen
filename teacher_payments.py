
import sqlite3

conn = sqlite3.connect('school.db')
c = conn.cursor()

c.execute('''
    CREATE TABLE IF NOT EXISTS teacher_payments (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        teacher_id INTEGER NOT NULL,
        amount REAL NOT NULL,
        term TEXT,
        year INTEGER,
        date_spent DATE DEFAULT CURRENT_DATE,
        recorded_by TEXT
    )
''')

conn.commit()
conn.close()
print("teacher_payments table created successfully.")
