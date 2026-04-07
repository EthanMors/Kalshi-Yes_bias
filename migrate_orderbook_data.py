"""migrate_orderbook_data.py

One-shot migration: move flat orderbook CSV files into the hierarchical layout.

Before:
    orderbook_data/orderbook_{CITY}_{STRIKE}_{YYYY-MM-DD}.csv

After:
    orderbook_data/{YYYY-MM-DD}/{CITY}/orderbook_{CITY}_{STRIKE}.csv

Run once, then discard.  errors.log and any unrecognised file are left untouched.
"""

import re
import shutil
from pathlib import Path

DATA_DIR = Path(__file__).parent / "orderbook_data"
PATTERN  = re.compile(r"^orderbook_([A-Z]+)_([\d.]+)_(\d{4}-\d{2}-\d{2})\.csv$")


def main() -> None:
    if not DATA_DIR.is_dir():
        print(f"Directory not found: {DATA_DIR}")
        return

    moved = 0

    for src in sorted(DATA_DIR.iterdir()):
        # Only consider immediate children that are files (skip subdirectories).
        if not src.is_file():
            continue

        m = PATTERN.match(src.name)
        if m is None:
            # Includes errors.log and anything else that doesn't match.
            continue

        city, strike, date_str = m.group(1), m.group(2), m.group(3)

        dst_dir = DATA_DIR / date_str / city
        dst_dir.mkdir(parents=True, exist_ok=True)

        dst = dst_dir / f"orderbook_{city}_{strike}.csv"

        shutil.move(str(src), str(dst))
        print(f"Moved: {src} -> {dst}")
        moved += 1

    print(f"Migration complete: {moved} files moved.")


if __name__ == "__main__":
    main()
