import sqlite3

conn = sqlite3.connect('school.db')
c = conn.cursor()

c.executemany('INSERT INTO classes (name, level) VALUES (?, ?)', [
    ('S1', 'O'), ('S2', 'O'), ('S3', 'O'), ('S4', 'O'),
    ('S5', 'A'), ('S6', 'A')
])

conn.commit()
conn.close()
print("Classes inserted successfully.")