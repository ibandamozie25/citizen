
import sqlite3
conn = sqlite3.connect('school.db') # or your DB path
c = conn.cursor()
c.execute("PRAGMA table_info(fees)").fetchall()
