"""Add durable report delivery outbox

Revision ID: 20260722_0010
Revises: 20260610_0009
Create Date: 2026-07-22

"""
from alembic import op
import sqlalchemy as sa


revision = "20260722_0010"
down_revision = "20260610_0009"
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        "report_outbox",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("scope_key", sa.String(), nullable=False),
        sa.Column("scope_label", sa.String(), nullable=False, server_default=""),
        sa.Column("scope_kind", sa.String(), nullable=False, server_default="group"),
        sa.Column("payload_json", sa.Text(), nullable=False),
        sa.Column("status", sa.String(), nullable=False, server_default="pending"),
        sa.Column("attempt_count", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("last_error", sa.Text(), nullable=True),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("updated_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("last_attempt_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("sent_at", sa.DateTime(timezone=True), nullable=True),
        sa.UniqueConstraint("scope_key", name="uq_report_outbox_scope_key"),
    )
    op.create_index("ix_report_outbox_scope_key", "report_outbox", ["scope_key"])
    op.create_index("ix_report_outbox_status", "report_outbox", ["status"])


def downgrade():
    op.drop_index("ix_report_outbox_status", table_name="report_outbox")
    op.drop_index("ix_report_outbox_scope_key", table_name="report_outbox")
    op.drop_table("report_outbox")
