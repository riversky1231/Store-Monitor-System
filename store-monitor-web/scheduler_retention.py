import datetime
import logging
import os

logger = logging.getLogger(__name__)


def resolve_product_retention_days(db, system_config_model, default_days: int) -> int:
    env_value = (os.getenv("STORE_MONITOR_RETENTION_DAYS") or "").strip()
    if env_value:
        try:
            parsed = int(env_value)
            if parsed >= 1:
                return parsed
        except ValueError:
            logger.warning("Invalid STORE_MONITOR_RETENTION_DAYS=%s, fallback to config.", env_value)

    config = db.query(system_config_model).first()
    if config and isinstance(config.product_retention_days, int) and config.product_retention_days >= 1:
        return config.product_retention_days
    return default_days


def prune_removed_products_history(
    session_factory,
    product_item_model,
    system_config_model,
    default_days: int,
) -> None:
    """Delete removed products older than retention days to control DB size."""
    db = session_factory()
    try:
        retention_days = resolve_product_retention_days(db, system_config_model, default_days)
        cutoff = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(days=retention_days)
        deleted_count = (
            db.query(product_item_model)
            .filter(product_item_model.removed_at.isnot(None))
            .filter(product_item_model.removed_at < cutoff)
            .delete(synchronize_session=False)
        )
        db.commit()
        logger.info(
            "Product history cleanup finished: removed=%d, retention_days=%d.",
            deleted_count,
            retention_days,
        )
    except Exception as exc:
        db.rollback()
        logger.error("Product history cleanup failed: %s", exc)
    finally:
        db.close()
