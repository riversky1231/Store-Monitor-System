"""Add missing tables and columns introduced after baseline.

Revision ID: 20260313_0002
Revises: 20260305_0001
Create Date: 2026-03-13
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa

revision = "20260313_0002"
down_revision = "20260305_0001"
branch_labels = None
depends_on = None


def _has_table(inspector: sa.Inspector, table_name: str) -> bool:
    return table_name in inspector.get_table_names()


def _has_column(inspector: sa.Inspector, table_name: str, column_name: str) -> bool:
    return any(col["name"] == column_name for col in inspector.get_columns(table_name))


def _has_index(inspector: sa.Inspector, table_name: str, index_name: str) -> bool:
    return any(idx["name"] == index_name for idx in inspector.get_indexes(table_name))


def upgrade() -> None:
    bind = op.get_bind()
    inspector = sa.inspect(bind)

    if not _has_table(inspector, "categories"):
        op.create_table(
            "categories",
            sa.Column("id", sa.Integer(), primary_key=True, nullable=False),
            sa.Column("name", sa.String(), nullable=True),
            sa.Column("created_at", sa.DateTime(timezone=True), nullable=True, server_default=sa.text("CURRENT_TIMESTAMP")),
        )
        op.create_index("ix_categories_id", "categories", ["id"], unique=False)
        op.create_index("ix_categories_name", "categories", ["name"], unique=True)
        inspector = sa.inspect(bind)
    else:
        if not _has_index(inspector, "categories", "ix_categories_id"):
            op.create_index("ix_categories_id", "categories", ["id"], unique=False)
            inspector = sa.inspect(bind)
        if not _has_index(inspector, "categories", "ix_categories_name"):
            op.create_index("ix_categories_name", "categories", ["name"], unique=True)
            inspector = sa.inspect(bind)

    if not _has_table(inspector, "pending_imports"):
        op.create_table(
            "pending_imports",
            sa.Column("id", sa.Integer(), primary_key=True, nullable=False),
            sa.Column("name", sa.String(), nullable=True),
            sa.Column("url", sa.String(), nullable=True),
            sa.Column("selector", sa.String(), nullable=True),
            sa.Column("check_interval_hours", sa.Integer(), nullable=True),
            sa.Column("recipients", sa.Text(), nullable=True),
            sa.Column("is_active", sa.Boolean(), nullable=True),
            sa.Column("created_at", sa.DateTime(timezone=True), nullable=True, server_default=sa.text("CURRENT_TIMESTAMP")),
        )
        op.create_index("ix_pending_imports_id", "pending_imports", ["id"], unique=False)
        inspector = sa.inspect(bind)
    elif not _has_index(inspector, "pending_imports", "ix_pending_imports_id"):
        op.create_index("ix_pending_imports_id", "pending_imports", ["id"], unique=False)
        inspector = sa.inspect(bind)

    if _has_table(inspector, "system_configs"):
        if not _has_column(inspector, "system_configs", "setup_complete"):
            op.add_column("system_configs", sa.Column("setup_complete", sa.Boolean(), nullable=True, server_default=sa.text("0")))
            inspector = sa.inspect(bind)
        if not _has_column(inspector, "system_configs", "admin_password_enc"):
            op.add_column("system_configs", sa.Column("admin_password_enc", sa.String(), nullable=True))
            inspector = sa.inspect(bind)

    if _has_table(inspector, "monitor_tasks"):
        if not _has_column(inspector, "monitor_tasks", "task_type"):
            op.add_column("monitor_tasks", sa.Column("task_type", sa.String(), nullable=True, server_default=sa.text("'search'")))
            inspector = sa.inspect(bind)
        if not _has_column(inspector, "monitor_tasks", "category"):
            op.add_column("monitor_tasks", sa.Column("category", sa.String(), nullable=True))
            inspector = sa.inspect(bind)
        if not _has_column(inspector, "monitor_tasks", "category_id"):
            op.add_column("monitor_tasks", sa.Column("category_id", sa.Integer(), nullable=True))
            inspector = sa.inspect(bind)

    if _has_table(inspector, "product_items"):
        if not _has_column(inspector, "product_items", "asin"):
            op.add_column("product_items", sa.Column("asin", sa.String(), nullable=True))
            inspector = sa.inspect(bind)


def downgrade() -> None:
    # SQLite downgrade for additive changes is intentionally omitted.
    pass
