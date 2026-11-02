"""Add manual review state to monitor tasks

Revision ID: 20260404_0007
Revises: 20260403_0006
Create Date: 2026-04-04

"""
from alembic import op
import sqlalchemy as sa


revision = "20260404_0007"
down_revision = "20260403_0006"
branch_labels = None
depends_on = None


def upgrade():
    with op.batch_alter_table("monitor_tasks", schema=None) as batch_op:
        batch_op.add_column(sa.Column("manual_review_pending", sa.Boolean(), nullable=True, server_default=sa.text("0")))
        batch_op.add_column(sa.Column("manual_review_kind", sa.String(), nullable=True))
        batch_op.add_column(sa.Column("manual_review_scope_key", sa.String(), nullable=True))
        batch_op.add_column(sa.Column("manual_review_scope_label", sa.String(), nullable=True))
        batch_op.add_column(sa.Column("manual_review_scope_kind", sa.String(), nullable=True))
        batch_op.add_column(sa.Column("manual_review_started_at", sa.DateTime(timezone=True), nullable=True))


def downgrade():
    with op.batch_alter_table("monitor_tasks", schema=None) as batch_op:
        batch_op.drop_column("manual_review_started_at")
        batch_op.drop_column("manual_review_scope_kind")
        batch_op.drop_column("manual_review_scope_label")
        batch_op.drop_column("manual_review_scope_key")
        batch_op.drop_column("manual_review_kind")
        batch_op.drop_column("manual_review_pending")
