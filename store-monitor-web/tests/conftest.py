import sys
from pathlib import Path

# Allow test modules to import from the store-monitor-web package root.
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
