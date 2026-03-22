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
