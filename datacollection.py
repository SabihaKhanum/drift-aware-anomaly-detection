"""
Download all 2022 daily BTCUSDT 1-minute klines files from Binance's public
historical data archive, extract them, and concatenate into a single CSV
ready for backtest_pipeline.py.

Usage:
    pip install requests
    python download_2022_btcusdt.py
"""

import io
import os
import sys
import time
import zipfile
from datetime import date, timedelta

import requests

YEAR = 2022
SYMBOL = "BTCUSDT"
INTERVAL = "1m"
OUTPUT_PATH = f"{SYMBOL}_{YEAR}_{INTERVAL}.csv"
BASE_URL = f"https://data.binance.vision/data/spot/daily/klines/{SYMBOL}/{INTERVAL}"

REQUEST_DELAY_SECONDS = 0.3   # be polite to the server between requests
MAX_RETRIES = 3


def daterange(start: date, end: date):
    """Yield every date from start to end (inclusive)."""
    current = start
    while current <= end:
        yield current
        current += timedelta(days=1)


def download_day(day: date) -> bytes | None:
    """Download one day's zip file. Returns raw zip bytes, or None on failure."""
    filename = f"{SYMBOL}-{INTERVAL}-{day.isoformat()}.zip"
    url = f"{BASE_URL}/{filename}"

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            # resp = requests.get(url, timeout=30)
            resp = requests.get(url, timeout=30, verify=False)
            if resp.status_code == 404:
                print(f"  [{day}] not found (404) -- skipping")
                return None
            resp.raise_for_status()
            return resp.content
        except requests.RequestException as e:
            print(f"  [{day}] attempt {attempt}/{MAX_RETRIES} failed: {e}")
            if attempt < MAX_RETRIES:
                time.sleep(1.5 * attempt)
    print(f"  [{day}] giving up after {MAX_RETRIES} attempts")
    return None


def extract_csv_rows(zip_bytes: bytes) -> list[str]:
    """Extract the CSV content from a zip file's bytes and return its lines."""
    with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
        csv_names = [n for n in zf.namelist() if n.endswith(".csv")]
        if not csv_names:
            raise ValueError("no .csv file found inside zip")
        with zf.open(csv_names[0]) as f:
            content = f.read().decode("utf-8")
    lines = [line for line in content.splitlines() if line.strip()]
    return lines


def main():
    start = date(YEAR, 1, 1)
    end = date(YEAR, 12, 31)

    total_days = (end - start).days + 1
    print(f"Downloading {SYMBOL} {INTERVAL} klines for {YEAR} ({total_days} days)")
    print(f"Output: {OUTPUT_PATH}\n")

    succeeded = 0
    failed_days = []
    total_rows = 0

    # Write incrementally so memory use stays flat even for a full year.
    with open(OUTPUT_PATH, "w", newline="") as out_f:
        for i, day in enumerate(daterange(start, end), start=1):
            print(f"[{i}/{total_days}] {day} ...", end=" ")

            zip_bytes = download_day(day)
            if zip_bytes is None:
                failed_days.append(day)
                continue

            try:
                lines = extract_csv_rows(zip_bytes)
            except (zipfile.BadZipFile, ValueError) as e:
                print(f"FAILED to extract: {e}")
                failed_days.append(day)
                continue

            for line in lines:
                out_f.write(line + "\n")
            total_rows += len(lines)
            succeeded += 1
            print(f"OK ({len(lines)} rows)")

            time.sleep(REQUEST_DELAY_SECONDS)

    print(f"\n=== DONE ===")
    print(f"Succeeded: {succeeded}/{total_days} days")
    print(f"Total rows written: {total_rows}")
    if failed_days:
        print(f"Failed/missing days ({len(failed_days)}): "
              f"{', '.join(str(d) for d in failed_days)}")
    print(f"\nOutput file: {os.path.abspath(OUTPUT_PATH)}")
    print("This is a headerless Binance klines CSV -- backtest_pipeline.py")
    print("auto-detects this format, so you can point BACKTEST_CSV_PATH")
    print("directly at it with no further editing.")


if __name__ == "__main__":
    main()