import datetime
import logging

logger = logging.getLogger(__name__)


def handle_empty_scrape_result(
    db,
    task,
    *,
    empty_alert_threshold: int,
    failure_threshold: int,
) -> None:
    """Handle case when scrape returns no products."""
    now = datetime.datetime.now(datetime.timezone.utc)
    empty_count = (task.consecutive_empty_count or 0) + 1
    failure_count = (task.consecutive_failure_count or 0) + 1
    task.consecutive_empty_count = empty_count
    task.consecutive_failure_count = failure_count
    task.last_failure_at = now
    task.last_failure_reason = "抓取结果为空"
    task.manual_review_pending = True
    if failure_count >= failure_threshold:
        task.health_state = "failed"
    else:
        task.health_state = "alert" if empty_count >= empty_alert_threshold else "warning"

    logger.warning(
        "Task %s: scrape returned 0 products (empty=%d/%d, failure=%d/%d).",
        task.name,
        empty_count,
        empty_alert_threshold,
        failure_count,
        failure_threshold,
    )

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
        # Manual-only mode sends one batch report instead of a second recovery email.
        task.last_recovery_at = now

    task.consecutive_empty_count = 0
    task.consecutive_failure_count = 0
    task.health_state = "healthy"
    task.manual_review_pending = False
    task.manual_review_kind = None
    task.manual_review_scope_key = None
    task.manual_review_scope_label = None
    task.manual_review_scope_kind = None
    task.manual_review_started_at = None
    task.last_failure_at = None
    task.last_failure_reason = None
    task.last_run_at = now
    task.next_run_at = None
    db.commit()

    if new_products or removed_products:
        logger.info(
            "Task %s: new=%d, removed=%d, queuing manual completion report.",
            task.name,
            len(new_products),
            len(removed_products),
        )
    elif is_first_successful_run:
        logger.info(
            "Task %s: baseline scrape completed with %d products. Queuing manual completion report.",
            task.name,
            len(current),
        )
    else:
        logger.info("Task %s: no changes detected; queuing manual completion report.", task.name)

    queue_digest_entry(
        task,
        new_products,
        removed_products,
        is_baseline=is_first_successful_run,
        baseline_count=len(current) if is_first_successful_run else 0,
        current_products=current,
        status="success",
    )
