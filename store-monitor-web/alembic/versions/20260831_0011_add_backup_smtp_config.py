"""Add backup SMTP configuration.

Revision ID: 20260831_0011
Revises: 20260722_0010
Create Date: 2026-08-31 13:45:00.000000
"""

from alembic import op
import sqlalchemy as sa


revision = "20260831_0011"
down_revision = "20260722_0010"
branch_labels = None
depends_on = None


def _has_column(inspector, table_name: str, column_name: str) -> bool:
    return any(column["name"] == column_name for column in inspector.get_columns(table_name))


def upgrade() -> None:
    bind = op.get_bind()
    inspector = sa.inspect(bind)
    with op.batch_alter_table("system_configs") as batch_op:
        if not _has_column(inspector, "system_configs", "backup_smtp_enabled"):
            batch_op.add_column(sa.Column("backup_smtp_enabled", sa.Boolean(), nullable=True, server_default=sa.text("0")))
        if not _has_column(inspector, "system_configs", "backup_smtp_server"):
            batch_op.add_column(sa.Column("backup_smtp_server", sa.String(), nullable=True))
        if not _has_column(inspector, "system_configs", "backup_smtp_port"):
            batch_op.add_column(sa.Column("backup_smtp_port", sa.Integer(), nullable=True, server_default="465"))
        if not _has_column(inspector, "system_configs", "backup_sender_email"):
            batch_op.add_column(sa.Column("backup_sender_email", sa.String(), nullable=True))
        if not _has_column(inspector, "system_configs", "backup_sender_password"):
            batch_op.add_column(sa.Column("backup_sender_password", sa.String(), nullable=True))


def downgrade() -> None:
    with op.batch_alter_table("system_configs") as batch_op:
        batch_op.drop_column("backup_sender_password")
        batch_op.drop_column("backup_sender_email")
        batch_op.drop_column("backup_smtp_port")
        batch_op.drop_column("backup_smtp_server")
        batch_op.drop_column("backup_smtp_enabled")
