"""Add integrity reference count to monitor tasks

Revision ID: 20260403_0006
Revises: 20260401_0005
Create Date: 2026-04-03

"""
from alembic import op
import sqlalchemy as sa


revision = "20260403_0006"
down_revision = "20260401_0005"
branch_labels = None
depends_on = None


def upgrade():
    with op.batch_alter_table("monitor_tasks", schema=None) as batch_op:
        batch_op.add_column(sa.Column("integrity_reference_count", sa.Integer(), nullable=True, server_default="0"))


def downgrade():
    with op.batch_alter_table("monitor_tasks", schema=None) as batch_op:
        batch_op.drop_column("integrity_reference_count")
