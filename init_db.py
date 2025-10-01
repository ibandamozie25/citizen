
# ====== TOP OF FILE (sec_app.py) ======
import os
import sqlite3
from datetime import datetime
from flask import Flask
from werkzeug.security import generate_password_hash

DB_PATH = "school.db"

app = Flask(__name__)
app.secret_key = os.environ.get("SECRET_KEY", "dev_secret_key")

def get_db_connection():
    """Return a SQLite connection with FK enforcement and row dicts."""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON;")
    return conn

def init_db():
    """Create/ensure all tables and seed admin user once."""
    conn = get_db_connection()
    c = conn.cursor()

    c.executescript("""
    -- Subjects
    CREATE TABLE IF NOT EXISTS subjects (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT NOT NULL,
        code TEXT
    );

    CREATE TABLE IF NOT EXISTS subject_papers (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        subject_id INTEGER NOT NULL,
        paper_name TEXT NOT NULL,
        paper_initial TEXT,
        FOREIGN KEY(subject_id) REFERENCES subjects(id) ON DELETE CASCADE
    );

    -- Employees (all staff)
    CREATE TABLE IF NOT EXISTS employees (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        first_name TEXT NOT NULL,
        middle_name TEXT,
        last_name TEXT NOT NULL,
        gender TEXT,
        contact TEXT,
        email TEXT,
        residence TEXT,
        department TEXT,
        designation TEXT,
        hire_date TEXT,
        status TEXT CHECK(status IN ('active','archived')) DEFAULT 'active',
        base_salary REAL DEFAULT 0.0,
        allowance REAL DEFAULT 0.0,
        bonus REAL DEFAULT 0.0,
        pay_cycle TEXT DEFAULT 'monthly',
        bank_name TEXT,
        bank_account TEXT,
        tin TEXT,
        notes TEXT,
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP
    );
    CREATE INDEX IF NOT EXISTS idx_employees_name ON employees(last_name, first_name);
    CREATE INDEX IF NOT EXISTS idx_employees_status ON employees(status);

    -- Teachers (linked 1:1 to employees when applicable)
    CREATE TABLE IF NOT EXISTS teachers (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        employee_id INTEGER UNIQUE,
        initials TEXT,
        subjects TEXT,
        class_name TEXT,
        can_reset_password INTEGER DEFAULT 0,
        status TEXT DEFAULT 'active',
        FOREIGN KEY(employee_id) REFERENCES employees(id) ON DELETE CASCADE
    );
    CREATE INDEX IF NOT EXISTS idx_teachers_status ON teachers(status);

    -- Users (system logins)
    CREATE TABLE IF NOT EXISTS users (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        username TEXT UNIQUE NOT NULL,
        password_hash TEXT NOT NULL,
        role TEXT CHECK(role IN ('admin','bursar','teacher','headteacher','director','clerk','deputyheadteacher','dos')) NOT NULL,
        status TEXT CHECK(status IN ('active','archived')) NOT NULL DEFAULT 'active',
        employee_id INTEGER,
        created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY(employee_id) REFERENCES employees(id)
    );
    CREATE INDEX IF NOT EXISTS idx_users_status ON users(status);
    CREATE INDEX IF NOT EXISTS idx_users_role ON users(role);
    CREATE INDEX IF NOT EXISTS idx_users_employee ON users(employee_id);

    -- Teacher ↔ Subject mapping
    CREATE TABLE IF NOT EXISTS teacher_subjects (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        teacher_id INTEGER NOT NULL,
        subject_id INTEGER NOT NULL,
        class_name TEXT NOT NULL,
        FOREIGN KEY(teacher_id) REFERENCES teachers(id) ON DELETE CASCADE,
        FOREIGN KEY(subject_id) REFERENCES subjects(id) ON DELETE CASCADE
    );

    -- Audit trail
    CREATE TABLE IF NOT EXISTS audit_trail (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER,
        action TEXT NOT NULL,
        timestamp DATETIME DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY(user_id) REFERENCES users(id) ON DELETE SET NULL
    );

    -- Classes & Streams
    CREATE TABLE IF NOT EXISTS classes (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        class_name TEXT NOT NULL,
        level TEXT NOT NULL,
        stream TEXT
    );

    CREATE TABLE IF NOT EXISTS streams (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT UNIQUE NOT NULL
    );

    -- Students
    CREATE TABLE IF NOT EXISTS students (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        first_name TEXT NOT NULL,
        middle_name TEXT,
        last_name TEXT NOT NULL,
        sex TEXT,
        year_completed INTEGER,
        class_name TEXT CHECK(class_name IN ('BABY','MIDDLE','TOP','P1','P2','P3','P4','P5','P6','P7')) NOT NULL DEFAULT 'BABY',
        stream TEXT,
        section TEXT CHECK(section IN ('day','boarding')) DEFAULT 'day',
        combination TEXT,
        fees_amount REAL,
        student_number TEXT UNIQUE,
        academic_year_id INTEGER,
        year_of_joining TEXT,
        term_joined TEXT,
        date_joined TEXT,
        cumulative_average TEXT,
        cumulative_grade TEXT,
        cumulative_comment TEXT,
        residence TEXT,
        house TEXT,
        parent_name TEXT,
        parent2_name TEXT,
        parent_contact TEXT,
        parent2_contact TEXT,
        fees_code TEXT,
        parent_email TEXT,
        archived INTEGER DEFAULT 0,
        current_class TEXT,
        status TEXT NOT NULL DEFAULT 'active' CHECK(status IN ('active','dropped','left','completed'))
    );
    CREATE INDEX IF NOT EXISTS idx_students_name ON students(last_name, first_name);

    -- Results (final)
    CREATE TABLE IF NOT EXISTS results (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        student_id INTEGER NOT NULL,
        subject_id INTEGER NOT NULL,
        eot REAL,
        total REAL,
        grade TEXT,
        comment TEXT,
        initials TEXT,
        term TEXT NOT NULL,
        year INTEGER NOT NULL,
        UNIQUE (student_id, subject_id, term, year),
        FOREIGN KEY (student_id) REFERENCES students(id) ON DELETE CASCADE,
        FOREIGN KEY (subject_id) REFERENCES subjects(id) ON DELETE CASCADE
    );
    CREATE INDEX IF NOT EXISTS idx_results_student_term ON results(student_id, term, year);

    -- Fees & Payments (fees + requirements recorded here)
    CREATE TABLE IF NOT EXISTS fees (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        student_id INTEGER,
        term TEXT,
        year INTEGER,
        amount_paid REAL,
        requirement_name TEXT,
        req_term TEXT,
        payment_item TEXT,
        bursary_amount REAL DEFAULT 0,
        carried_forward REAL DEFAULT 0,
        expected_amount REAL,
        date_paid TEXT,
        method TEXT DEFAULT 'N/A',
        payment_type TEXT DEFAULT 'school_fees',
        FOREIGN KEY(student_id) REFERENCES students(id) ON DELETE CASCADE
    );
    CREATE INDEX IF NOT EXISTS idx_fees_student_term ON fees(student_id, term, year);

    -- Requirements (per class per term)
    CREATE TABLE IF NOT EXISTS requirements (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        class_name TEXT,
        term TEXT,
        name TEXT,
        qty INTEGER,
        amount REAL DEFAULT 0
    );

    -- Bursaries
    CREATE TABLE IF NOT EXISTS bursaries (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        student_id INTEGER,
        sponsor_name TEXT,
        amount REAL,
        term TEXT,
        year INTEGER,
        FOREIGN KEY(student_id) REFERENCES students(id) ON DELETE CASCADE
    );
    CREATE INDEX IF NOT EXISTS idx_bursaries_student ON bursaries(student_id);

    -- Academic years & Term dates
    CREATE TABLE IF NOT EXISTS academic_years (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        year TEXT UNIQUE NOT NULL,
        current_term TEXT DEFAULT 'Term 1',
        is_active INTEGER DEFAULT 0
    );

    CREATE TABLE IF NOT EXISTS term_dates (
        year INTEGER NOT NULL,
        term TEXT NOT NULL,
        next_term TEXT,
        next_term_date TEXT,
        PRIMARY KEY (year, term)
    );

    -- Midterms / Holiday package
    CREATE TABLE IF NOT EXISTS midterms (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        student_id INTEGER NOT NULL,
        term TEXT NOT NULL,
        year INTEGER NOT NULL,
        assessment TEXT NOT NULL, -- 'Beginning'/'Mid'/'Holiday Package'
        eng REAL, 
        mat REAL, 
        sci REAL, 
        sst REAL,
        agg INTEGER, 
        total INTEGER,
        UNIQUE (student_id, term, year, assessment),
        FOREIGN KEY(student_id) REFERENCES students(id) ON DELETE CASCADE
    );

    -- Report comments
    CREATE TABLE IF NOT EXISTS report_comments (
        student_id INTEGER NOT NULL,
        term TEXT NOT NULL,
        year INTEGER NOT NULL,
        teacher_comment TEXT,
        head_comment TEXT,
        PRIMARY KEY (student_id, term, year),
        FOREIGN KEY(student_id) REFERENCES students(id) ON DELETE CASCADE
    );

    -- Class fees
    CREATE TABLE IF NOT EXISTS class_fees (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        class_name TEXT,
        level TEXT,
        section TEXT CHECK(section IN ('day','boarding')),
        amount REAL
    );

    -- Expenses
    CREATE TABLE IF NOT EXISTS expense_categories (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT NOT NULL UNIQUE
    );

    CREATE TABLE IF NOT EXISTS expenses (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        description TEXT,
        amount REAL NOT NULL,
        term TEXT,
        year INTEGER,
        date_spent DATE DEFAULT CURRENT_DATE,
        category_id INTEGER,
        recorded_by TEXT,
        type TEXT, -- 'staff_pay','other', etc
        FOREIGN KEY(category_id) REFERENCES expense_categories(id) ON DELETE SET NULL
    );
    CREATE INDEX IF NOT EXISTS idx_expenses_term_year ON expenses(term, year);

    -- Grading scale
    CREATE TABLE IF NOT EXISTS grading_scale (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        grade TEXT NOT NULL,
        lower_limit INTEGER NOT NULL,
        upper_limit INTEGER NOT NULL,
        comment TEXT
    );

    -- Record score (working/in-progress)
    CREATE TABLE IF NOT EXISTS record_score (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        student_id INTEGER NOT NULL,
        subject_id INTEGER NOT NULL,
        term TEXT NOT NULL,
        initials TEXT,
        year INTEGER NOT NULL,
        bot_mark INTEGER,
        midterm_mark INTEGER,
        eot_mark INTEGER,
        average_mark REAL,
        grade TEXT,
        comment TEXT,
        processed_on DATETIME DEFAULT CURRENT_TIMESTAMP,
        UNIQUE (student_id, subject_id, term, year),
        FOREIGN KEY(student_id) REFERENCES students(id) ON DELETE CASCADE,
        FOREIGN KEY(subject_id) REFERENCES subjects(id) ON DELETE CASCADE
    );

    -- Other income
    CREATE TABLE IF NOT EXISTS other_income (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        source TEXT,
        amount REAL,
        term TEXT,
        year INTEGER,
        description TEXT,
        recorded_by TEXT,
        date_received DATETIME DEFAULT CURRENT_TIMESTAMP
    );

    -- Payroll (employee-based; teacher_id kept for legacy)
    CREATE TABLE IF NOT EXISTS payroll (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        employee_id INTEGER,
        teacher_id INTEGER,
        term TEXT NOT NULL,
        year INTEGER NOT NULL,
        expected_salary REAL NOT NULL,
        bonus REAL DEFAULT 0,
        allowance REAL DEFAULT 0,
        total REAL NOT NULL,
        paid_amount REAL DEFAULT 0,
        status TEXT CHECK(status IN('fully_paid','partially_paid','not_paid')) NOT NULL DEFAULT 'not_paid',
        date_paid TEXT DEFAULT (DATE('now')),
        FOREIGN KEY(employee_id) REFERENCES employees(id),
        FOREIGN KEY(teacher_id) REFERENCES teachers(id)
    );
    CREATE INDEX IF NOT EXISTS idx_payroll_emp ON payroll(employee_id);

    -- Assets
    CREATE TABLE IF NOT EXISTS assets (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT,
        model TEXT,
        value REAL DEFAULT 0.0,
        year_purchased TEXT,
        condition TEXT,
        qty INTEGER,
        archived_reason TEXT
    );

    -- Class comments
    CREATE TABLE IF NOT EXISTS class_comments (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        class_name TEXT,
        term TEXT,
        year INTEGER,
        comment TEXT
    );

    -- Archived students
    CREATE TABLE IF NOT EXISTS archived_students (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        student_id INTEGER NOT NULL,
        student_number TEXT,
        full_name TEXT,
        class_name TEXT,
        year_completed INTEGER NOT NULL,
        completed_stage TEXT,
        outstanding_balance REAL DEFAULT 0,
        archived_on DATETIME DEFAULT CURRENT_TIMESTAMP,
        FOREIGN KEY(student_id) REFERENCES students(id) ON DELETE SET NULL
    );

    -- Reports snapshot (optional)
    CREATE TABLE IF NOT EXISTS reports (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        student_id INTEGER,
        class_name TEXT NOT NULL,
        stream TEXT,
        subject_id INTEGER NOT NULL,
        term TEXT,
        year INTEGER,
        average_mark REAL,
        grade TEXT,
        comment TEXT,
        teacher_remark TEXT,
        headteacher_remark TEXT,
        teacher_id INTEGER,
        bot_mark INTEGER,
        midterm_mark INTEGER,
        eot_mark INTEGER,
        teacher_initial TEXT,
        processed_on DATETIME DEFAULT CURRENT_TIMESTAMP,
        UNIQUE (student_id, subject_id, year, term),
        FOREIGN KEY(student_id) REFERENCES students(id) ON DELETE CASCADE,
        FOREIGN KEY(subject_id) REFERENCES subjects(id) ON DELETE CASCADE
    );
    """)

    # Defaults / seeds
    c.execute("INSERT OR IGNORE INTO expense_categories(name) VALUES ('Salaries')")

    # Admin user (admin / admin123)
    row = c.execute("SELECT 1 FROM users WHERE username = 'admin'").fetchone()
    if not row:
        c.execute(
            "INSERT INTO users (username, password_hash, role, status, created_at) VALUES (?, ?, ?, ?, datetime('now'))
                ''', ('admin', generate_password_hash('admin123'), 'admin', 'active'))

    conn.commit()
    conn.close()
    print("Database initialized.")

# tiny health route to verify app runs
@app.get("/health")
def health():
    return {"ok": True, "db": os.path.exists(DB_PATH)}

# ====== END TOP OF FILE BLOCK ======
