from __future__ import annotations

import datetime
import threading
from collections import defaultdict
from typing import DefaultDict, Dict, List

_RESOURCE_ORDER = (
    "document",
    "stylesheet",
    "script",
    "image",
    "font",
    "xhr",
    "fetch",
    "media",
    "websocket",
    "manifest",
    "other",
)


def _now_utc() -> datetime.datetime:
    return datetime.datetime.now(datetime.timezone.utc)


def _normalize_resource_type(resource_type: str | None) -> str:
    value = (resource_type or "").strip().lower()
    return value if value else "other"


def _format_bytes(value: int) -> str:
    size = max(0, int(value or 0))
    gb_value = size / float(1024 ** 3)
    if gb_value >= 100:
        return f"{gb_value:.1f} GB"
    if gb_value >= 1:
        return f"{gb_value:.2f} GB"
    return f"{gb_value:.4f} GB"


def _format_duration(seconds: float) -> str:
    total_seconds = max(0, int(seconds or 0))
    minutes, sec = divmod(total_seconds, 60)
    hours, minutes = divmod(minutes, 60)
    if hours:
        return f"{hours}h {minutes}m"
    if minutes:
        return f"{minutes}m {sec}s"
    return f"{sec}s"


class TrafficCapture:
    def __init__(self, task_id: int, task_name: str, task_url: str) -> None:
        self.task_id = task_id
        self.task_name = task_name
        self.task_url = task_url
        self.started_at = _now_utc()
        self.completed_at: datetime.datetime | None = None
        self.status = "running"

        self._lock = threading.Lock()
        self._request_count = 0
        self._upload_bytes = 0
        self._download_bytes = 0
        self._resource_request_counts: DefaultDict[str, int] = defaultdict(int)
        self._resource_download_bytes: DefaultDict[str, int] = defaultdict(int)
        self._unknown_response_count = 0
        self._finalized = False

    def record_request(self, resource_type: str | None, upload_bytes: int = 0) -> None:
        normalized = _normalize_resource_type(resource_type)
        with self._lock:
            self._request_count += 1
            self._upload_bytes += max(0, int(upload_bytes or 0))
            self._resource_request_counts[normalized] += 1

    def record_response(self, resource_type: str | None, download_bytes: int = 0, measured: bool = True) -> None:
        normalized = _normalize_resource_type(resource_type)
        with self._lock:
            if measured:
                self._download_bytes += max(0, int(download_bytes or 0))
                self._resource_download_bytes[normalized] += max(0, int(download_bytes or 0))
            else:
                self._unknown_response_count += 1

    def finalize(self, status: str) -> None:
        with self._lock:
            if self._finalized:
                return
            self._finalized = True
            self.status = status
            self.completed_at = _now_utc()

    def snapshot(self) -> dict:
        with self._lock:
            started_at = self.started_at
            completed_at = self.completed_at
            duration_seconds = (
                (completed_at or _now_utc()) - started_at
            ).total_seconds()

            resource_rows: List[dict] = []
            ordered_keys = list(_RESOURCE_ORDER)
            for key in self._resource_request_counts.keys():
                if key not in ordered_keys:
                    ordered_keys.append(key)

            for resource_type in ordered_keys:
                request_count = int(self._resource_request_counts.get(resource_type, 0) or 0)
                download_bytes = int(self._resource_download_bytes.get(resource_type, 0) or 0)
                if request_count == 0 and download_bytes == 0:
                    continue
                resource_rows.append(
                    {
                        "type": resource_type,
                        "request_count": request_count,
                        "download_bytes": download_bytes,
                        "download_human": _format_bytes(download_bytes),
                    }
                )

            return {
                "task_id": self.task_id,
                "task_name": self.task_name,
                "task_url": self.task_url,
                "status": self.status,
                "started_at": started_at.isoformat(),
                "completed_at": completed_at.isoformat() if completed_at else None,
                "duration_seconds": duration_seconds,
                "duration_label": _format_duration(duration_seconds),
                "request_count": int(self._request_count),
                "upload_bytes": int(self._upload_bytes),
                "upload_human": _format_bytes(self._upload_bytes),
                "download_bytes": int(self._download_bytes),
                "download_human": _format_bytes(self._download_bytes),
                "unknown_response_count": int(self._unknown_response_count),
                "resource_breakdown": resource_rows,
            }


class _TrafficRegistry:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._started_at = _now_utc()
        self._active: Dict[int, TrafficCapture] = {}
        self._last_completed: dict | None = None
        self._total_request_count = 0
        self._total_upload_bytes = 0
        self._total_download_bytes = 0
        self._completed_task_count = 0

    def start_capture(self, task_id: int, task_name: str, task_url: str) -> TrafficCapture:
        capture = TrafficCapture(task_id=task_id, task_name=task_name, task_url=task_url)
        with self._lock:
            self._active[task_id] = capture
        return capture

    def finish_capture(self, capture: TrafficCapture | None, status: str) -> dict | None:
        if capture is None:
            return None

        capture.finalize(status)
        snapshot = capture.snapshot()

        with self._lock:
            self._active.pop(capture.task_id, None)
            self._last_completed = snapshot
            self._total_request_count += int(snapshot["request_count"] or 0)
            self._total_upload_bytes += int(snapshot["upload_bytes"] or 0)
            self._total_download_bytes += int(snapshot["download_bytes"] or 0)
            self._completed_task_count += 1
        return snapshot

    def get_snapshot(self) -> dict:
        with self._lock:
            active_snapshots = [capture.snapshot() for capture in self._active.values()]
            active_snapshots.sort(key=lambda item: item["started_at"], reverse=True)
            last_completed = dict(self._last_completed) if self._last_completed else None
            return {
                "started_at": self._started_at.isoformat(),
                "started_since_label": _format_duration((_now_utc() - self._started_at).total_seconds()),
                "totals": {
                    "request_count": int(self._total_request_count),
                    "upload_bytes": int(self._total_upload_bytes),
                    "upload_human": _format_bytes(self._total_upload_bytes),
                    "download_bytes": int(self._total_download_bytes),
                    "download_human": _format_bytes(self._total_download_bytes),
                    "completed_task_count": int(self._completed_task_count),
                },
                "active_task": active_snapshots[0] if active_snapshots else None,
                "last_completed_task": last_completed,
            }


_registry = _TrafficRegistry()


def start_capture(task_id: int, task_name: str, task_url: str) -> TrafficCapture:
    return _registry.start_capture(task_id=task_id, task_name=task_name, task_url=task_url)


def finish_capture(capture: TrafficCapture | None, status: str) -> dict | None:
    return _registry.finish_capture(capture=capture, status=status)


def get_snapshot() -> dict:
    return _registry.get_snapshot()
