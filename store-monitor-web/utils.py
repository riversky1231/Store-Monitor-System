import sys
from pathlib import Path


def get_resource_path(relative_path: str) -> str:
    """Get absolute path to resource, works for dev and for PyInstaller.

    Args:
        relative_path: The relative path to the resource. Must not contain
                      path traversal sequences (../ or absolute paths).

    Returns:
        Absolute path to the resource within the base directory.

    Raises:
        ValueError: If relative_path contains path traversal sequences.
    """
    rel_path = Path(relative_path)
    if rel_path.is_absolute() or rel_path.drive:
        raise ValueError(f"Invalid relative_path: path traversal not allowed: {relative_path}")
    if any(part == ".." for part in rel_path.parts):
        raise ValueError(f"Invalid relative_path: path traversal not allowed: {relative_path}")

    try:
        # PyInstaller creates a temp folder and stores path in _MEIPASS
        base_path = Path(sys._MEIPASS)
    except Exception:
        base_path = Path.cwd()

    full_path = (base_path / rel_path).resolve()
    base_real = base_path.resolve()
    try:
        full_path.relative_to(base_real)
    except ValueError as exc:
        raise ValueError(f"Invalid relative_path: path traversal not allowed: {relative_path}") from exc

    return str(full_path)
