"""Add permanent seen-products registry for lifetime new-product dedup

Revision ID: 20260610_0009
Revises: 20260501_0008
Create Date: 2026-06-10

"""
from alembic import op
import sqlalchemy as sa


revision = "20260610_0009"
down_revision = "20260501_0008"
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        "seen_products",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("asin", sa.String(), nullable=True),
        sa.Column("product_link", sa.String(), nullable=True),
        sa.Column("name", sa.String(), nullable=True),
        sa.Column(
            "first_seen_at",
            sa.DateTime(timezone=True),
            server_default=sa.func.now(),
            nullable=True,
        ),
        sa.UniqueConstraint("asin", name="uq_seen_products_asin"),
        sa.UniqueConstraint("product_link", name="uq_seen_products_link"),
    )
    op.create_index("ix_seen_products_asin", "seen_products", ["asin"])
    op.create_index("ix_seen_products_link", "seen_products", ["product_link"])

    # Backfill from existing product history so already-known products are
    # never reported as "new" after this upgrade.
    op.execute(
        """
        INSERT OR IGNORE INTO seen_products (asin, product_link, name, first_seen_at)
        SELECT UPPER(TRIM(asin)), MIN(product_link), MIN(name), MIN(COALESCE(discovered_at, CURRENT_TIMESTAMP))
        FROM product_items
        WHERE asin IS NOT NULL AND TRIM(asin) != ''
        GROUP BY UPPER(TRIM(asin))
        """
    )
    op.execute(
        """
        INSERT OR IGNORE INTO seen_products (asin, product_link, name, first_seen_at)
        SELECT NULL, product_link, MIN(name), MIN(COALESCE(discovered_at, CURRENT_TIMESTAMP))
        FROM product_items
        WHERE (asin IS NULL OR TRIM(asin) = '')
          AND product_link IS NOT NULL AND TRIM(product_link) != ''
        GROUP BY product_link
        """
    )


def downgrade():
    op.drop_index("ix_seen_products_link", table_name="seen_products")
    op.drop_index("ix_seen_products_asin", table_name="seen_products")
    op.drop_table("seen_products")
