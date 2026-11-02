"""Add persistent ordering for tasks inside each group.

Revision ID: 20260920_0012
Revises: 20260831_0011
Create Date: 2026-09-20 15:00:00.000000
"""

from alembic import op
import sqlalchemy as sa


revision = "20260920_0012"
down_revision = "20260831_0011"
branch_labels = None
depends_on = None


def _has_column(inspector, table_name: str, column_name: str) -> bool:
    return any(column["name"] == column_name for column in inspector.get_columns(table_name))


def upgrade() -> None:
    bind = op.get_bind()
    inspector = sa.inspect(bind)
    if not _has_column(inspector, "monitor_tasks", "sort_order"):
        with op.batch_alter_table("monitor_tasks", schema=None) as batch_op:
            batch_op.add_column(
                sa.Column("sort_order", sa.Integer(), nullable=False, server_default="0")
            )

    # Preserve the historical ID order as the initial custom order. A
    # correlated count works on the SQLite versions supported by the app.
    op.execute(
        sa.text(
            """
            UPDATE monitor_tasks AS current_task
            SET sort_order = (
                SELECT COUNT(*) - 1
                FROM monitor_tasks AS earlier_task
                WHERE (
                    earlier_task.category_id = current_task.category_id
                    OR (
                        earlier_task.category_id IS NULL
                        AND current_task.category_id IS NULL
                    )
                )
                AND earlier_task.id <= current_task.id
            )
            """
        )
    )


def downgrade() -> None:
    bind = op.get_bind()
    inspector = sa.inspect(bind)
    if _has_column(inspector, "monitor_tasks", "sort_order"):
        with op.batch_alter_table("monitor_tasks", schema=None) as batch_op:
            batch_op.drop_column("sort_order")
