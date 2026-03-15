"""Add proxy_url to system_configs.

Revision ID: 20260313_0003
Revises: 20260313_0002
Create Date: 2026-03-13
"""

from __future__ import annotations

from alembic import op
import sqlalchemy as sa

revision = "20260313_0003"
down_revision = "20260313_0002"
branch_labels = None
depends_on = None


def _has_column(inspector: sa.Inspector, table_name: str, column_name: str) -> bool:
    return any(col["name"] == column_name for col in inspector.get_columns(table_name))


def upgrade() -> None:
    bind = op.get_bind()
    inspector = sa.inspect(bind)
    if not _has_column(inspector, "system_configs", "proxy_url"):
        op.add_column("system_configs", sa.Column("proxy_url", sa.String(), nullable=True))


def downgrade() -> None:
    # SQLite downgrade for additive changes is intentionally omitted.
    pass
