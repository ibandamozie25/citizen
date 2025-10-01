
import os
from pathlib import Path


class BaseConfig:
    # ---------------------
    # Security & Logging
    # ---------------------
    SECRET_KEY = os.getenv("SECRET_KEY", "dev-secret-change-me")
    LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")

    # ---------------------
    # Database
    # ---------------------
    DB_BACKEND = os.getenv("DB_BACKEND", "sqlite")

    # SQLite default
    SQLITE_PATH = os.getenv("SQLITE_PATH", str(Path.cwd() / "school.db"))

    # MySQL optional
    MYSQL_URL = os.getenv("MYSQL_URL", "")

    # ---------------------
    # Receipt / School Info
    # ---------------------
    RECEIPT_CHARS = int(os.getenv("RECEIPT_CHARS", 48)) # 42 or 48

    SCHOOL_NAME = os.getenv(
        "SCHOOL_NAME", "CITIZENS DAY AND BOARDING PRIMARY SCHOOL"
    )
    SCHOOL_ADDRESS_LINE1 = os.getenv(
        "SCHOOL_ADDRESS_LINE1", "P.O. Box 31882, Kampala"
    )
    SCHOOL_TAGLINE = os.getenv("SCHOOL_TAGLINE", "Strive for the best")

    # Optional extra header lines (safe defaults to empty)
    SCHOOL_NAME_LINE1 = os.getenv("SCHOOL_NAME_LINE1", "")
    SCHOOL_NAME_LINE2 = os.getenv("SCHOOL_NAME_LINE2", "")
    SCHOOL_POBOX_LINE = os.getenv("SCHOOL_POBOX_LINE", "")

    # Printer & logo (safe defaults ensure no crash)
    RECEIPT_PRINTER_NAME = os.getenv(
        "RECEIPT_PRINTER_NAME", r"GP-80220(Cut) Series"
    )
    RECEIPT_LOGO_PATH = os.getenv("RECEIPT_LOGO_PATH", "static/logo.jpg")
    RECEIPT_PAPER_DOTS = int(os.getenv("RECEIPT_PAPER_DOTS", 576)) # 80mm ~576, 58mm ~384

    # Hard cap for logo width (dots). Always min(PAPER_DOTS, MAX_DOTS)
    RECEIPT_LOGO_MAX_DOTS = int(os.getenv("RECEIPT_LOGO_MAX_DOTS", 200))


class DevConfig(BaseConfig):
    DEBUG = True


class ProdConfig(BaseConfig):
    DEBUG = False
    TESTING = False


class TestConfig(BaseConfig):
    TESTING = True
    LOG_LEVEL = "WARNING"
    # keep tests isolated
    SQLITE_PATH = str(Path.cwd() / "test_school.db")
