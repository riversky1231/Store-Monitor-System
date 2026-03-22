import os
import logging
from pathlib import Path

from sqlalchemy import create_engine, text, DDL
from sqlalchemy.orm import declarative_base, sessionmaker

from utils import get_runtime_base_path

logger = logging.getLogger(__name__)


def _resolve_database_path() -> Path:
    configured = (os.getenv("MONITOR_WEB_DB_PATH") or "").strip()
    if configured:
        configured_path = Path(configured).expanduser()
        if not configured_path.is_absolute():
            configured_path = (get_runtime_base_path() / configured_path).resolve()
        configured_path.parent.mkdir(parents=True, exist_ok=True)
        return configured_path

    cwd_path = (Path.cwd() / "monitor.db").resolve()
    runtime_path = (get_runtime_base_path() / "monitor.db").resolve()
    if cwd_path.exists():
        return cwd_path
    if runtime_path.exists():
        return runtime_path
    runtime_path.parent.mkdir(parents=True, exist_ok=True)
    return runtime_path


DB_PATH = _resolve_database_path()
SQLALCHEMY_DATABASE_URL = f"sqlite:///{DB_PATH.as_posix()}"

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
        _normalize_product_history_for_current_policy()
        return
    _run_legacy_migrations()
    _normalize_product_history_for_current_policy()


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
        _ensure_column(conn, "system_configs", "smtp_server", "VARCHAR")
        _ensure_column(conn, "system_configs", "smtp_port", "INTEGER DEFAULT 465")
        _ensure_column(conn, "system_configs", "sender_email", "VARCHAR")
        _ensure_column(conn, "system_configs", "sender_password", "VARCHAR")
        _ensure_column(conn, "product_items", "removed_at", "DATETIME")
        _ensure_column(conn, "product_items", "miss_count", "INTEGER DEFAULT 0")
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
        _ensure_column(conn, "monitor_tasks", "peak_product_count", "INTEGER DEFAULT 0")
        _migrate_legacy_system_config_columns(conn)


def _removal_tracking_enabled() -> bool:
    return (os.getenv("MONITOR_WEB_TRACK_REMOVALS") or "").strip().lower() in ("1", "true", "yes")


def _normalize_product_history_for_current_policy() -> int:
    with engine.connect() as conn:
        updated = _normalize_product_history_for_current_policy_conn(conn, _removal_tracking_enabled())
        if updated:
            logger.info(
                "Normalized %d historical product rows for current removal-tracking policy.",
                updated,
            )
        return updated


def _normalize_product_history_for_current_policy_conn(conn, track_removals: bool) -> int:
    if track_removals:
        return 0

    existing_cols = {row[1] for row in conn.execute(text("PRAGMA table_info(product_items)"))}
    if not existing_cols:
        return 0

    assignments: list[str] = []
    predicates: list[str] = []

    if "removed_at" in existing_cols:
        assignments.append("removed_at = NULL")
        predicates.append("removed_at IS NOT NULL")

    if "miss_count" in existing_cols:
        assignments.append("miss_count = 0")
        predicates.append("COALESCE(miss_count, 0) != 0")

    if not assignments or not predicates:
        return 0

    # 使用参数化方式构建 SET 和 WHERE 子句，字段名来自已校验的白名单列集合
    set_clause = ", ".join(assignments)
    where_clause = " OR ".join(predicates)
    result = conn.execute(
        text(f"UPDATE product_items SET {set_clause} WHERE {where_clause}")
    )
    conn.commit()
    return int(result.rowcount or 0)


def _ensure_column(conn, table_name: str, column_name: str, column_type_sql: str) -> None:
    # Whitelist validation to prevent SQL injection
    _ALLOWED_TABLES = {
        "product_items", "system_configs", "monitor_tasks",
        "categories", "pending_imports"
    }
    _ALLOWED_COLUMNS = {
        "product_items": {"url", "name", "price", "currency", "rating", "review_count",
                         "availability", "last_updated", "task_id", "asin", "removed_at", "miss_count"},
        "system_configs": {"smtp_server", "smtp_host", "smtp_port", "smtp_user",
                          "sender_email", "sender_password", "smtp_password_enc",
                          "smtp_from_email", "smtp_to_emails", "product_retention_days",
                          "setup_complete", "admin_password_enc", "proxy_url"},
        "monitor_tasks": {"name", "url", "max_pages", "category", "enabled",
                         "last_run_at", "last_status", "last_error", "consecutive_empty_count",
                         "health_state", "last_health_alert_at", "last_recovery_at",
                         "category_id", "task_type", "peak_product_count"},
        "categories": {"name", "description"},
        "pending_imports": {"source_db_path", "imported_at", "status"}
    }
    _ALLOWED_COLUMN_TYPES = {
        "DATETIME",
        "INTEGER DEFAULT 90",
        "INTEGER DEFAULT 0",
        "INTEGER DEFAULT 465",
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

    # 使用 SQLAlchemy DDL 接口执行 ALTER TABLE，所有标识符均已经过白名单校验
    ddl = DDL(f"ALTER TABLE {table_name} ADD COLUMN {column_name} {column_type_sql}")
    conn.execute(ddl)
    conn.commit()


def _migrate_legacy_system_config_columns(conn) -> None:
    """Backfill renamed system-config columns from older schema names when present."""
    existing_cols = {row[1] for row in conn.execute(text("PRAGMA table_info(system_configs)"))}
    if not existing_cols:
        return

    assignments: list[str] = []
    if {"smtp_server", "smtp_host"}.issubset(existing_cols):
        assignments.append(
            "smtp_server = CASE "
            "WHEN COALESCE(TRIM(smtp_server), '') = '' THEN smtp_host "
            "ELSE smtp_server END"
        )
    if {"sender_email", "smtp_from_email"}.issubset(existing_cols):
        assignments.append(
            "sender_email = CASE "
            "WHEN COALESCE(TRIM(sender_email), '') = '' THEN smtp_from_email "
            "ELSE sender_email END"
        )
    elif {"sender_email", "smtp_user"}.issubset(existing_cols):
        assignments.append(
            "sender_email = CASE "
            "WHEN COALESCE(TRIM(sender_email), '') = '' THEN smtp_user "
            "ELSE sender_email END"
        )
    if {"sender_password", "smtp_password_enc"}.issubset(existing_cols):
        assignments.append(
            "sender_password = CASE "
            "WHEN COALESCE(TRIM(sender_password), '') = '' THEN smtp_password_enc "
            "ELSE sender_password END"
        )

    if not assignments:
        return

    conn.execute(text(f"UPDATE system_configs SET {', '.join(assignments)}"))
    conn.commit()
