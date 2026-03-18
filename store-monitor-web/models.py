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
    product_retention_days = Column(Integer, default=90)
    setup_complete = Column(Boolean, default=False)
    admin_password_enc = Column(String, nullable=True)  # Fernet-encrypted admin password
    proxy_url = Column(String, nullable=True)
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
    is_active = Column(Boolean, default=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    last_run_at = Column(DateTime(timezone=True), nullable=True)
    next_run_at = Column(DateTime(timezone=True), nullable=True)
    consecutive_empty_count = Column(Integer, default=0)
    health_state = Column(String, default="healthy")  # healthy|warning|alert
    last_health_alert_at = Column(DateTime(timezone=True), nullable=True)
    last_recovery_at = Column(DateTime(timezone=True), nullable=True)
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


class ProductItem(Base):
    __tablename__ = "product_items"

    id = Column(Integer, primary_key=True, index=True)
    task_id = Column(Integer, ForeignKey("monitor_tasks.id"))
    product_link = Column(String, index=True)
    asin = Column(String, nullable=True, index=True)  # Amazon Standard Identification Number
    name = Column(String)
    discovered_at = Column(DateTime(timezone=True), server_default=func.now())
    removed_at = Column(DateTime(timezone=True), nullable=True)  # set when product disappears from store
    miss_count = Column(Integer, default=0)  # Consecutive times not detected (for removal confirmation)

    task = relationship("MonitorTask", back_populates="products")
