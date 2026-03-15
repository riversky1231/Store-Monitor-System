"""Schema baseline with health tracking and retention controls.

Revision ID: 20260305_0001
Revises:
Create Date: 2026-03-05
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision = "20260305_0001"
down_revision = None
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

    if not _has_table(inspector, "system_configs"):
        op.create_table(
            "system_configs",
            sa.Column("id", sa.Integer(), primary_key=True, nullable=False),
            sa.Column("smtp_server", sa.String(), nullable=True),
            sa.Column("smtp_port", sa.Integer(), nullable=True),
            sa.Column("sender_email", sa.String(), nullable=True),
            sa.Column("sender_password", sa.String(), nullable=True),
            sa.Column("product_retention_days", sa.Integer(), nullable=True, server_default=sa.text("90")),
            sa.Column("updated_at", sa.DateTime(timezone=True), nullable=True),
        )
        inspector = sa.inspect(bind)
    elif not _has_column(inspector, "system_configs", "product_retention_days"):
        op.add_column(
            "system_configs",
            sa.Column("product_retention_days", sa.Integer(), nullable=True, server_default=sa.text("90")),
        )
        inspector = sa.inspect(bind)

    if _has_table(inspector, "system_configs") and not _has_index(inspector, "system_configs", "ix_system_configs_id"):
        op.create_index("ix_system_configs_id", "system_configs", ["id"], unique=False)

    if not _has_table(inspector, "monitor_tasks"):
        op.create_table(
            "monitor_tasks",
            sa.Column("id", sa.Integer(), primary_key=True, nullable=False),
            sa.Column("name", sa.String(), nullable=True),
            sa.Column("url", sa.String(), nullable=True),
            sa.Column("selector", sa.String(), nullable=True),
            sa.Column("check_interval_hours", sa.Integer(), nullable=True),
            sa.Column("recipients", sa.Text(), nullable=True),
            sa.Column("is_active", sa.Boolean(), nullable=True),
            sa.Column("created_at", sa.DateTime(timezone=True), nullable=True),
            sa.Column("last_run_at", sa.DateTime(timezone=True), nullable=True),
            sa.Column("next_run_at", sa.DateTime(timezone=True), nullable=True),
            sa.Column("consecutive_empty_count", sa.Integer(), nullable=True, server_default=sa.text("0")),
            sa.Column("health_state", sa.String(), nullable=True, server_default=sa.text("'healthy'")),
            sa.Column("last_health_alert_at", sa.DateTime(timezone=True), nullable=True),
            sa.Column("last_recovery_at", sa.DateTime(timezone=True), nullable=True),
        )
        inspector = sa.inspect(bind)
    else:
        if not _has_column(inspector, "monitor_tasks", "consecutive_empty_count"):
            op.add_column(
                "monitor_tasks",
                sa.Column("consecutive_empty_count", sa.Integer(), nullable=True, server_default=sa.text("0")),
            )
            inspector = sa.inspect(bind)
        if not _has_column(inspector, "monitor_tasks", "health_state"):
            op.add_column(
                "monitor_tasks",
                sa.Column("health_state", sa.String(), nullable=True, server_default=sa.text("'healthy'")),
            )
            inspector = sa.inspect(bind)
        if not _has_column(inspector, "monitor_tasks", "last_health_alert_at"):
            op.add_column("monitor_tasks", sa.Column("last_health_alert_at", sa.DateTime(timezone=True), nullable=True))
            inspector = sa.inspect(bind)
        if not _has_column(inspector, "monitor_tasks", "last_recovery_at"):
            op.add_column("monitor_tasks", sa.Column("last_recovery_at", sa.DateTime(timezone=True), nullable=True))
            inspector = sa.inspect(bind)

    if _has_table(inspector, "monitor_tasks") and not _has_index(inspector, "monitor_tasks", "ix_monitor_tasks_id"):
        op.create_index("ix_monitor_tasks_id", "monitor_tasks", ["id"], unique=False)
    if _has_table(inspector, "monitor_tasks") and not _has_index(inspector, "monitor_tasks", "ix_monitor_tasks_name"):
        op.create_index("ix_monitor_tasks_name", "monitor_tasks", ["name"], unique=False)

    if not _has_table(inspector, "product_items"):
        op.create_table(
            "product_items",
            sa.Column("id", sa.Integer(), primary_key=True, nullable=False),
            sa.Column("task_id", sa.Integer(), sa.ForeignKey("monitor_tasks.id"), nullable=True),
            sa.Column("product_link", sa.String(), nullable=True),
            sa.Column("name", sa.String(), nullable=True),
            sa.Column("discovered_at", sa.DateTime(timezone=True), nullable=True),
            sa.Column("removed_at", sa.DateTime(timezone=True), nullable=True),
        )
        inspector = sa.inspect(bind)
    elif not _has_column(inspector, "product_items", "removed_at"):
        op.add_column("product_items", sa.Column("removed_at", sa.DateTime(timezone=True), nullable=True))
        inspector = sa.inspect(bind)

    if _has_table(inspector, "product_items") and not _has_index(inspector, "product_items", "ix_product_items_id"):
        op.create_index("ix_product_items_id", "product_items", ["id"], unique=False)
    if _has_table(inspector, "product_items") and not _has_index(inspector, "product_items", "ix_product_items_product_link"):
        op.create_index("ix_product_items_product_link", "product_items", ["product_link"], unique=False)


def downgrade() -> None:
    # SQLite downgrade for additive columns is intentionally omitted.
    pass
