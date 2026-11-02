"""Add delayed retry schedule to report outbox.

Revision ID: 20260929_0013
Revises: 20260920_0012
"""

from alembic import op
import sqlalchemy as sa


revision = "20260929_0013"
down_revision = "20260920_0012"
branch_labels = None
depends_on = None


def upgrade():
    with op.batch_alter_table("report_outbox") as batch_op:
        batch_op.add_column(sa.Column("next_attempt_at", sa.DateTime(timezone=True), nullable=True))
        batch_op.create_index("ix_report_outbox_next_attempt_at", ["next_attempt_at"])


def downgrade():
    with op.batch_alter_table("report_outbox") as batch_op:
        batch_op.drop_index("ix_report_outbox_next_attempt_at")
        batch_op.drop_column("next_attempt_at")
