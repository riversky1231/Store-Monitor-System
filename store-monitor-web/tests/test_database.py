from pathlib import Path

from alembic import command
from alembic.config import Config
from sqlalchemy import create_engine, text

from database import (
    _ensure_column,
    _migrate_legacy_system_config_columns,
    _normalize_product_history_for_current_policy_conn,
)


def test_normalize_product_history_clears_removed_and_miss_when_removals_disabled():
    engine = create_engine("sqlite:///:memory:")
    with engine.connect() as conn:
        conn.execute(
            text(
                """
                CREATE TABLE product_items (
                    id INTEGER PRIMARY KEY,
                    name VARCHAR,
                    removed_at DATETIME,
                    miss_count INTEGER DEFAULT 0
                )
                """
            )
        )
        conn.execute(
            text(
                """
                INSERT INTO product_items (id, name, removed_at, miss_count) VALUES
                (1, 'A', '2026-03-18 10:00:00', 2),
                (2, 'B', NULL, 3),
                (3, 'C', NULL, 0)
                """
            )
        )
        conn.commit()

        updated = _normalize_product_history_for_current_policy_conn(conn, track_removals=False)
        rows = conn.execute(
            text("SELECT id, removed_at, miss_count FROM product_items ORDER BY id")
        ).fetchall()

    assert updated == 2
    assert rows == [
        (1, None, 0),
        (2, None, 0),
        (3, None, 0),
    ]


def test_normalize_product_history_noop_when_removals_enabled():
    engine = create_engine("sqlite:///:memory:")
    with engine.connect() as conn:
        conn.execute(
            text(
                """
                CREATE TABLE product_items (
                    id INTEGER PRIMARY KEY,
                    removed_at DATETIME,
                    miss_count INTEGER DEFAULT 0
                )
                """
            )
        )
        conn.execute(
            text(
                "INSERT INTO product_items (id, removed_at, miss_count) VALUES (1, '2026-03-18 10:00:00', 2)"
            )
        )
        conn.commit()

        updated = _normalize_product_history_for_current_policy_conn(conn, track_removals=True)
        row = conn.execute(
            text("SELECT removed_at, miss_count FROM product_items WHERE id = 1")
        ).fetchone()

    assert updated == 0
    assert row == ("2026-03-18 10:00:00", 2)


def test_ensure_column_accepts_current_system_config_columns():
    engine = create_engine("sqlite:///:memory:")
    with engine.connect() as conn:
        conn.execute(text("CREATE TABLE system_configs (id INTEGER PRIMARY KEY)"))
        conn.commit()

        _ensure_column(conn, "system_configs", "smtp_server", "VARCHAR")
        _ensure_column(conn, "system_configs", "smtp_port", "INTEGER DEFAULT 465")
        _ensure_column(conn, "system_configs", "sender_email", "VARCHAR")
        _ensure_column(conn, "system_configs", "sender_password", "VARCHAR")

        columns = {row[1] for row in conn.execute(text("PRAGMA table_info(system_configs)"))}

    assert {"smtp_server", "smtp_port", "sender_email", "sender_password"}.issubset(columns)


def test_ensure_column_accepts_product_new_discovery_flag():
    engine = create_engine("sqlite:///:memory:")
    with engine.connect() as conn:
        conn.execute(text("CREATE TABLE product_items (id INTEGER PRIMARY KEY)"))
        conn.commit()

        _ensure_column(conn, "product_items", "is_new_discovery", "BOOLEAN DEFAULT 0")

        columns = {row[1] for row in conn.execute(text("PRAGMA table_info(product_items)"))}

    assert "is_new_discovery" in columns


def test_ensure_column_accepts_task_sort_order():
    engine = create_engine("sqlite:///:memory:")
    with engine.connect() as conn:
        conn.execute(text("CREATE TABLE monitor_tasks (id INTEGER PRIMARY KEY)"))
        conn.commit()

        _ensure_column(conn, "monitor_tasks", "sort_order", "INTEGER DEFAULT 0")

        columns = {row[1] for row in conn.execute(text("PRAGMA table_info(monitor_tasks)"))}
        default_value = conn.execute(
            text("SELECT dflt_value FROM pragma_table_info('monitor_tasks') WHERE name = 'sort_order'")
        ).scalar_one()

    assert "sort_order" in columns
    assert default_value == "0"


def test_task_sort_order_migration_backfills_id_order_per_group(tmp_path):
    database_path = tmp_path / "migration.db"
    project_dir = Path(__file__).resolve().parents[1]
    config = Config(str(project_dir / "alembic.ini"))
    config.set_main_option("script_location", str(project_dir / "alembic"))
    config.set_main_option("sqlalchemy.url", f"sqlite:///{database_path.as_posix()}")

    command.upgrade(config, "20260831_0011")
    engine = create_engine(f"sqlite:///{database_path.as_posix()}")
    with engine.begin() as conn:
        conn.execute(text("INSERT INTO categories (id, name) VALUES (1, 'A'), (2, 'B')"))
        conn.execute(
            text(
                """
                INSERT INTO monitor_tasks (id, name, category_id) VALUES
                (10, 'A-1', 1),
                (20, 'B-1', 2),
                (30, 'A-2', 1),
                (40, 'A-3', 1),
                (50, 'B-2', 2)
                """
            )
        )
    engine.dispose()

    command.upgrade(config, "head")
    engine = create_engine(f"sqlite:///{database_path.as_posix()}")
    with engine.connect() as conn:
        rows = conn.execute(
            text("SELECT id, category_id, sort_order FROM monitor_tasks ORDER BY id")
        ).fetchall()
    engine.dispose()

    assert rows == [
        (10, 1, 0),
        (20, 2, 0),
        (30, 1, 1),
        (40, 1, 2),
        (50, 2, 1),
    ]


def test_migrate_legacy_system_config_columns_backfills_renamed_fields():
    engine = create_engine("sqlite:///:memory:")
    with engine.connect() as conn:
        conn.execute(
            text(
                """
                CREATE TABLE system_configs (
                    id INTEGER PRIMARY KEY,
                    smtp_host VARCHAR,
                    smtp_from_email VARCHAR,
                    smtp_password_enc VARCHAR,
                    smtp_server VARCHAR,
                    sender_email VARCHAR,
                    sender_password VARCHAR
                )
                """
            )
        )
        conn.execute(
            text(
                """
                INSERT INTO system_configs (
                    id, smtp_host, smtp_from_email, smtp_password_enc,
                    smtp_server, sender_email, sender_password
                ) VALUES (
                    1, 'smtp.legacy.example.com', 'legacy@example.com', 'enc::legacy',
                    '', '', ''
                )
                """
            )
        )
        conn.commit()

        _migrate_legacy_system_config_columns(conn)
        row = conn.execute(
            text(
                """
                SELECT smtp_server, sender_email, sender_password
                FROM system_configs
                WHERE id = 1
                """
            )
        ).fetchone()

    assert row == ("smtp.legacy.example.com", "legacy@example.com", "enc::legacy")
