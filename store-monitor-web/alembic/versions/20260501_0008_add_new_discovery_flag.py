"""Add new-discovery flag to products

Revision ID: 20260501_0008
Revises: 20260404_0007
Create Date: 2026-05-01

"""
from alembic import op
import sqlalchemy as sa


revision = "20260501_0008"
down_revision = "20260404_0007"
branch_labels = None
depends_on = None


def upgrade():
    with op.batch_alter_table("product_items", schema=None) as batch_op:
        batch_op.add_column(
            sa.Column(
                "is_new_discovery",
                sa.Boolean(),
                nullable=False,
                server_default=sa.text("0"),
            )
        )


def downgrade():
    with op.batch_alter_table("product_items", schema=None) as batch_op:
        batch_op.drop_column("is_new_discovery")
