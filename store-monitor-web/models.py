import datetime

from sqlalchemy import Boolean, Column, Integer, String, DateTime, ForeignKey, Text
from sqlalchemy.sql import func
from sqlalchemy.orm import relationship
from database import Base


class SystemConfig(Base):
    __tablename__ = "system_configs"

    id = Column(Integer, primary_key=True, index=True)
    smtp_server = Column(String, default="smtp.qq.com")
    smtp_port = Column(Integer, default=465)
    sender_email = Column(String, default="")
    sender_password = Column(String, default="")  # stores encrypted token: enc::<fernet-token>
    backup_smtp_enabled = Column(Boolean, default=False)
    backup_smtp_server = Column(String, default="")
    backup_smtp_port = Column(Integer, default=465)
    backup_sender_email = Column(String, default="")
    backup_sender_password = Column(String, default="")
    product_retention_days = Column(Integer, default=90)
    setup_complete = Column(Boolean, default=False)
    admin_password_enc = Column(String, nullable=True)  # Fernet-encrypted admin password
    proxy_url = Column(String, nullable=True)
    last_daily_report_at = Column(DateTime(timezone=True), nullable=True)
    updated_at = Column(DateTime(timezone=True), onupdate=func.now(), default=func.now())


class Category(Base):
    __tablename__ = "categories"

    id = Column(Integer, primary_key=True, index=True)
    name = Column(String, unique=True, index=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now())

    tasks = relationship("MonitorTask", back_populates="group", cascade="all, delete-orphan")


class MonitorTask(Base):
    __tablename__ = "monitor_tasks"

    id = Column(Integer, primary_key=True, index=True)
    name = Column(String, index=True)
    url = Column(String)
    task_type = Column(String, default="search")  # search | storefront
    selector = Column(String, default="div.product-item")
    check_interval_hours = Column(Integer, default=6)
    recipients = Column(Text) # Comma-separated emails
    category_id = Column(Integer, ForeignKey("categories.id"), nullable=True)
    category = Column(String, nullable=True)  # Legacy text label (kept for migration)
    sort_order = Column(Integer, nullable=False, default=0, server_default="0")
    is_active = Column(Boolean, default=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    last_run_at = Column(DateTime(timezone=True), nullable=True)
    next_run_at = Column(DateTime(timezone=True), nullable=True)
    consecutive_empty_count = Column(Integer, default=0)
    consecutive_failure_count = Column(Integer, default=0)
    health_state = Column(String, default="healthy")  # healthy|warning|alert|failed
    manual_review_pending = Column(Boolean, default=False)
    manual_review_kind = Column(String, nullable=True)  # integrity|failure
    manual_review_scope_key = Column(String, nullable=True)
    manual_review_scope_label = Column(String, nullable=True)
    manual_review_scope_kind = Column(String, nullable=True)
    manual_review_started_at = Column(DateTime(timezone=True), nullable=True)
    last_health_alert_at = Column(DateTime(timezone=True), nullable=True)
    last_recovery_at = Column(DateTime(timezone=True), nullable=True)
    last_failure_at = Column(DateTime(timezone=True), nullable=True)
    last_failure_reason = Column(String, nullable=True)
    integrity_reference_count = Column(Integer, default=0)  # Trusted baseline for integrity checks
    peak_product_count = Column(Integer, default=0)  # Historical max product count for integrity check

    group = relationship("Category", back_populates="tasks")
    products = relationship("ProductItem", back_populates="task", cascade="all, delete-orphan")


class PendingImport(Base):
    """Temporary holding table for stores from legacy .db files awaiting group assignment."""
    __tablename__ = "pending_imports"

    id = Column(Integer, primary_key=True, index=True)
    name = Column(String)
    url = Column(String)
    selector = Column(String, default="div[data-component-type='s-search-result']")
    check_interval_hours = Column(Integer, default=24)
    recipients = Column(Text)
    is_active = Column(Boolean, default=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now())


class ReportOutbox(Base):
    """Durable email report waiting for delivery or retained for status history."""

    __tablename__ = "report_outbox"

    id = Column(Integer, primary_key=True, index=True)
    scope_key = Column(String, nullable=False, unique=True, index=True)
    scope_label = Column(String, nullable=False, default="")
    scope_kind = Column(String, nullable=False, default="group")
    payload_json = Column(Text, nullable=False)
    status = Column(String, nullable=False, default="pending", index=True)
    attempt_count = Column(Integer, nullable=False, default=0)
    last_error = Column(Text, nullable=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now(), nullable=False)
    updated_at = Column(
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
        nullable=False,
    )
    last_attempt_at = Column(DateTime(timezone=True), nullable=True)
    next_attempt_at = Column(DateTime(timezone=True), nullable=True, index=True)
    sent_at = Column(DateTime(timezone=True), nullable=True)


class SeenProduct(Base):
    """Permanent registry of every product the system has ever scraped.

    Unlike ProductItem rows (which are deleted when a task/group is removed or
    pruned by the retention job), this table is never cleaned up, so "new
    product" notifications stay accurate for the whole lifetime of the system:
    once an ASIN/link has been seen in any scrape, it can never be reported as
    new again — even if it goes out of stock and is re-listed later.
    """
    __tablename__ = "seen_products"

    id = Column(Integer, primary_key=True, index=True)
    asin = Column(String, nullable=True, unique=True, index=True)
    product_link = Column(String, nullable=True, unique=True, index=True)
    name = Column(String, nullable=True)
    first_seen_at = Column(
        DateTime(timezone=True),
        default=lambda: datetime.datetime.now(datetime.timezone.utc),
        server_default=func.now(),
    )


class ProductItem(Base):
    __tablename__ = "product_items"

    id = Column(Integer, primary_key=True, index=True)
    task_id = Column(Integer, ForeignKey("monitor_tasks.id"))
    product_link = Column(String, index=True)
    asin = Column(String, nullable=True, index=True)  # Amazon Standard Identification Number
    name = Column(String)
    discovered_at = Column(
        DateTime(timezone=True),
        default=lambda: datetime.datetime.now(datetime.timezone.utc),
        server_default=func.now(),
    )
    is_new_discovery = Column(Boolean, default=False, server_default="0")
    removed_at = Column(DateTime(timezone=True), nullable=True)  # set when product disappears from store
    miss_count = Column(Integer, default=0)  # Consecutive times not detected (for removal confirmation)

    task = relationship("MonitorTask", back_populates="products")
