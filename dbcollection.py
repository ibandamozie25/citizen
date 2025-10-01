import sqlite3

def migrate_database(db_path):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    try:
        cursor.executescript("""
        PRAGMA foreign_keys=off;
        BEGIN TRANSACTION;

        -- Example fix for 'users' table
        ALTER TABLE users RENAME TO users_old;

        CREATE TABLE users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            username TEXT UNIQUE NOT NULL,
            email TEXT,
            password TEXT NOT NULL,
            first_name TEXT NOT NULL,
            middle_name TEXT,
            last_name TEXT NOT NULL,
            contact TEXT,
            residence TEXT,
            level_teach TEXT,
            initials TEXT,
            role TEXT CHECK(role IN('admin','bursar','teacher','headteacher','director')) NOT NULL,
            status TEXT CHECK(status IN('active','archived')) DEFAULT 'active',
            actions TEXT,
            date_paid TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );

        INSERT INTO users (
            id, username, email, password, first_name, middle_name, last_name, contact, residence, level_teach, initials, role, status, actions, date_paid
        )
        SELECT 
            id, username, email, password, first_name, middle_name, last_name, contact, residence, level_teach, initials, role, status, actions, date_paid
        FROM users_old;

        DROP TABLE users_old;

        COMMIT;
        PRAGMA foreign_keys=on;
        """)
        print("Migration completed successfully.")
    except sqlite3.Error as e:
        print(f"An error occurred: {e}")
        conn.rollback()
    finally:
        conn.close()

if __name__ == "__main__":
    db_path = "your_database.db"  # Replace with your database file path
    migrate_database(db_path)
