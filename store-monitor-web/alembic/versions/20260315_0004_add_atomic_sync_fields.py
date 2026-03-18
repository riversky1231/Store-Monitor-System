"""Add fields for atomic sync: peak_product_count and miss_count

Revision ID: 20260315_0004
Revises: 20260313_0003
Create Date: 2026-03-15

"""
from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision = '20260315_0004'
down_revision = '20260313_0003'
branch_labels = None
depends_on = None


def upgrade():
    # Add peak_product_count to monitor_tasks
    with op.batch_alter_table('monitor_tasks', schema=None) as batch_op:
        batch_op.add_column(sa.Column('peak_product_count', sa.Integer(), nullable=True, server_default='0'))
    
    # Add miss_count to product_items
    with op.batch_alter_table('product_items', schema=None) as batch_op:
        batch_op.add_column(sa.Column('miss_count', sa.Integer(), nullable=True, server_default='0'))


def downgrade():
    with op.batch_alter_table('product_items', schema=None) as batch_op:
        batch_op.drop_column('miss_count')
    
    with op.batch_alter_table('monitor_tasks', schema=None) as batch_op:
        batch_op.drop_column('peak_product_count')
