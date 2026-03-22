import datetime
import logging

logger = logging.getLogger(__name__)


def handle_empty_scrape_result(
    db,
    task,
    *,
    empty_alert_threshold: int,
    send_health_alert,
) -> None:
    """Handle case when scrape returns no products."""
    now = datetime.datetime.now(datetime.timezone.utc)
    count = (task.consecutive_empty_count or 0) + 1
    task.consecutive_empty_count = count
    task.health_state = "alert" if count >= empty_alert_threshold else "warning"

    logger.warning(
        "Task %s: scrape returned 0 products (consecutive=%d/%d).",
        task.name,
        count,
        empty_alert_threshold,
    )

    if count == empty_alert_threshold:
        task.last_health_alert_at = now
        logger.error(
            "Task %s reached %d consecutive empty scrapes. Sending health alert.",
            task.name,
            count,
        )
        db.commit()
        send_health_alert(db, task, count)
    else:
        db.commit()


def handle_successful_scrape(
    db,
    task,
    current,
    new_products,
    removed_products,
    is_first_successful_run: bool,
    *,
    empty_alert_threshold: int,
    send_recovery_notification,
    queue_digest_entry,
    is_queue_worker_thread,
    send_email,
) -> None:
    """Handle successful scrape result."""
    now = datetime.datetime.now(datetime.timezone.utc)

    prev_count = task.consecutive_empty_count or 0
    if prev_count >= empty_alert_threshold:
        send_recovery_notification(db, task, prev_count)
        task.last_recovery_at = now

    task.consecutive_empty_count = 0
    task.health_state = "healthy"
    task.last_run_at = now
    task.next_run_at = now + datetime.timedelta(hours=task.check_interval_hours)
    db.commit()

    if new_products or removed_products:
        logger.info(
            "Task %s: new=%d, removed=%d, queuing for digest.",
            task.name,
            len(new_products),
            len(removed_products),
        )
        queue_digest_entry(task.name, new_products, removed_products, is_baseline=False)
    elif is_first_successful_run:
        logger.info(
            "Task %s: baseline scrape completed with %d products. Queuing initial digest.",
            task.name,
            len(current),
        )
        if is_queue_worker_thread():
            queue_digest_entry(task.name, [], [], is_baseline=True, baseline_count=len(current))
        else:
            send_email(db, task, current, [])
    else:
        logger.info("Task %s: no changes detected.", task.name)
