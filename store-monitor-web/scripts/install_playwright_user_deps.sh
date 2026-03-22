#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
TARGET_DIR="${1:-$ROOT_DIR/playwright-libs}"
DEB_DIR="$TARGET_DIR/.debs"

mkdir -p "$DEB_DIR"
rm -f "$DEB_DIR"/*.deb

packages=(
  libnss3
  libnspr4
  libasound2
)

echo "Downloading user-space Playwright runtime libraries into: $TARGET_DIR"
(cd "$DEB_DIR" && apt download "${packages[@]}")

for deb in "$DEB_DIR"/*.deb; do
  dpkg-deb -x "$deb" "$TARGET_DIR"
done

echo
echo "Playwright browser libraries are ready."
echo "Detected library path:"
if [[ -d "$TARGET_DIR/usr/lib/x86_64-linux-gnu" ]]; then
  echo "  $TARGET_DIR/usr/lib/x86_64-linux-gnu"
elif [[ -d "$TARGET_DIR/lib/x86_64-linux-gnu" ]]; then
  echo "  $TARGET_DIR/lib/x86_64-linux-gnu"
else
  echo "  (expected library directory not found)"
fi

echo
echo "The scraper auto-detects these extracted libraries on the next run."
