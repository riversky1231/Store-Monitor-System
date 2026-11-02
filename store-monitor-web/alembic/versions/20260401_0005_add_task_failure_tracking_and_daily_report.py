"""Add task failure tracking fields and daily report timestamp

Revision ID: 20260401_0005
Revises: 20260315_0004
Create Date: 2026-04-01

"""
from alembic import op
import sqlalchemy as sa


revision = "20260401_0005"
down_revision = "20260315_0004"
branch_labels = None
depends_on = None


def upgrade():
    with op.batch_alter_table("monitor_tasks", schema=None) as batch_op:
        batch_op.add_column(sa.Column("consecutive_failure_count", sa.Integer(), nullable=True, server_default="0"))
        batch_op.add_column(sa.Column("last_failure_at", sa.DateTime(timezone=True), nullable=True))
        batch_op.add_column(sa.Column("last_failure_reason", sa.String(), nullable=True))

    with op.batch_alter_table("system_configs", schema=None) as batch_op:
        batch_op.add_column(sa.Column("last_daily_report_at", sa.DateTime(timezone=True), nullable=True))


def downgrade():
    with op.batch_alter_table("system_configs", schema=None) as batch_op:
        batch_op.drop_column("last_daily_report_at")

    with op.batch_alter_table("monitor_tasks", schema=None) as batch_op:
        batch_op.drop_column("last_failure_reason")
        batch_op.drop_column("last_failure_at")
        batch_op.drop_column("consecutive_failure_count")
