import logging
from pathlib import Path

from sqlalchemy import create_engine, text
from sqlalchemy.orm import declarative_base, sessionmaker

SQLALCHEMY_DATABASE_URL = "sqlite:///./monitor.db"
logger = logging.getLogger(__name__)

engine = create_engine(
    SQLALCHEMY_DATABASE_URL, connect_args={"check_same_thread": False}
)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

Base = declarative_base()

# Dependency
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


def run_migrations():
    """Run Alembic migrations when available, with safe fallback for packaged mode."""
    if _run_alembic_migrations():
        return
    _run_legacy_migrations()


def _run_alembic_migrations() -> bool:
    try:
        from alembic import command
        from alembic.config import Config
    except ImportError:
        return False

    root_dir = Path(__file__).resolve().parent
    ini_path = root_dir / "alembic.ini"
    script_path = root_dir / "alembic"
    if not ini_path.exists() or not script_path.exists():
        return False

    try:
        cfg = Config(str(ini_path))
        cfg.set_main_option("script_location", script_path.as_posix())
        cfg.set_main_option("sqlalchemy.url", SQLALCHEMY_DATABASE_URL)
        command.upgrade(cfg, "head")
        return True
    except Exception as exc:
        logger.error("Alembic migration failed, falling back to legacy migration path: %s", exc)
        return False


def _run_legacy_migrations() -> None:
    """Apply additive schema changes for environments that cannot run Alembic."""
    Base.metadata.create_all(bind=engine)  # Also creates pending_imports table.
    with engine.connect() as conn:
        _ensure_column(conn, "product_items", "removed_at", "DATETIME")
        _ensure_column(conn, "system_configs", "product_retention_days", "INTEGER DEFAULT 90")
        _ensure_column(conn, "monitor_tasks", "consecutive_empty_count", "INTEGER DEFAULT 0")
        _ensure_column(conn, "monitor_tasks", "health_state", "VARCHAR DEFAULT 'healthy'")
        _ensure_column(conn, "monitor_tasks", "last_health_alert_at", "DATETIME")
        _ensure_column(conn, "monitor_tasks", "last_recovery_at", "DATETIME")
        _ensure_column(conn, "system_configs", "setup_complete", "BOOLEAN DEFAULT 0")
        _ensure_column(conn, "system_configs", "admin_password_enc", "VARCHAR")
        _ensure_column(conn, "system_configs", "proxy_url", "VARCHAR")
        _ensure_column(conn, "monitor_tasks", "category", "VARCHAR")
        _ensure_column(conn, "monitor_tasks", "category_id", "INTEGER REFERENCES categories(id)")
        _ensure_column(conn, "product_items", "asin", "VARCHAR")
        _ensure_column(conn, "monitor_tasks", "task_type", "VARCHAR DEFAULT 'search'")


def _ensure_column(conn, table_name: str, column_name: str, column_type_sql: str) -> None:
    # Whitelist validation to prevent SQL injection
    _ALLOWED_TABLES = {
        "product_items", "system_configs", "monitor_tasks",
        "categories", "pending_imports"
    }
    _ALLOWED_COLUMNS = {
        "product_items": {"url", "name", "price", "currency", "rating", "review_count",
                         "availability", "last_updated", "task_id", "asin", "removed_at"},
        "system_configs": {"smtp_host", "smtp_port", "smtp_user", "smtp_password_enc",
                          "smtp_from_email", "smtp_to_emails", "product_retention_days",
                          "setup_complete", "admin_password_enc", "proxy_url"},
        "monitor_tasks": {"name", "url", "max_pages", "category", "enabled",
                         "last_run_at", "last_status", "last_error", "consecutive_empty_count",
                         "health_state", "last_health_alert_at", "last_recovery_at",
                         "category_id", "task_type"},
        "categories": {"name", "description"},
        "pending_imports": {"source_db_path", "imported_at", "status"}
    }
    _ALLOWED_COLUMN_TYPES = {
        "DATETIME",
        "INTEGER DEFAULT 90",
        "INTEGER DEFAULT 0",
        "VARCHAR DEFAULT 'healthy'",
        "BOOLEAN DEFAULT 0",
        "VARCHAR",
        "INTEGER REFERENCES categories(id)",
    }

    # Validate table name against whitelist
    if table_name not in _ALLOWED_TABLES:
        raise ValueError(f"Invalid table name: {table_name}")

    # Validate column name against whitelist
    allowed_cols = _ALLOWED_COLUMNS.get(table_name, set())
    if column_name not in allowed_cols:
        raise ValueError(f"Invalid column name '{column_name}' for table '{table_name}'")

    if column_type_sql not in _ALLOWED_COLUMN_TYPES:
        raise ValueError(f"Invalid column type for '{table_name}.{column_name}': {column_type_sql}")

    existing_cols = {row[1] for row in conn.execute(text(f"PRAGMA table_info({table_name})"))}
    if column_name in existing_cols:
        return
    conn.execute(text(f"ALTER TABLE {table_name} ADD COLUMN {column_name} {column_type_sql}"))
    conn.commit()
