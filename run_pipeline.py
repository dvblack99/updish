#!/usr/bin/env python3
"""
run_pipeline.py
Master pipeline runner. Runs all stages in order.
Designed to be called by cron weekly.

Cron setup (run weekly on Sunday at 3am):
    crontab -e
    0 3 * * 0 cd /root/mysite/html/updish && python3 run_pipeline.py >> /var/log/updish.log 2>&1
"""

import subprocess
import sys
import time
import os

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))

STAGES = [
    ("Syncing CoV licences",     "build_db.py"),
    ("Scraping websites",        "scrape_html.py"),
    ("Exporting data.js",        "export_data_js.py"),
]


def run_stage(label, script):
    path = os.path.join(SCRIPT_DIR, script)
    print(f"\n{'='*50}")
    print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] {label}")
    print(f"{'='*50}")
    result = subprocess.run([sys.executable, path], capture_output=False)
    if result.returncode != 0:
        print(f"ERROR: {script} failed with code {result.returncode}")
        return False
    return True


if __name__ == "__main__":
    print(f"UpDish pipeline started: {time.strftime('%Y-%m-%d %H:%M:%S')}")
    start = time.time()

    for label, script in STAGES:
        ok = run_stage(label, script)
        if not ok:
            print("Pipeline aborted.")
            sys.exit(1)

    elapsed = int(time.time() - start)
    print(f"\nPipeline complete in {elapsed}s: {time.strftime('%Y-%m-%d %H:%M:%S')}")
