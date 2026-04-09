#!/usr/bin/env python3
"""
scrape_html.py
Fetches HTML for businesses that have a website but haven't been scraped
in the last 30 days. Stores raw HTML in the database.

Run manually or via cron. Safe to interrupt and re-run.
"""

import sqlite3
import urllib.request
import urllib.error
import time
import os
import sys

DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "updish.db")
STALE_DAYS = 30
TIMEOUT = 15
DELAY = 1.0  # seconds between requests, be polite

HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; UpDish/1.0; +https://davidjamesblack.com/updish)",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-CA,en;q=0.9",
}


def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    return conn


def fetch_html(url):
    """Fetch URL, return (html, final_url, status). Returns None html on failure."""
    if not url.startswith("http"):
        url = "https://" + url
    try:
        req = urllib.request.Request(url, headers=HEADERS)
        with urllib.request.urlopen(req, timeout=TIMEOUT) as resp:
            final_url = resp.geturl()
            raw = resp.read()
            # try decode
            charset = "utf-8"
            ct = resp.headers.get("Content-Type", "")
            if "charset=" in ct:
                charset = ct.split("charset=")[-1].strip().split(";")[0].strip()
            try:
                html = raw.decode(charset, errors="replace")
            except (LookupError, UnicodeDecodeError):
                html = raw.decode("utf-8", errors="replace")
            return html, final_url, "ok"
    except urllib.error.HTTPError as e:
        return None, url, f"http_{e.code}"
    except urllib.error.URLError as e:
        return None, url, f"error_{str(e.reason)[:40]}"
    except Exception as e:
        return None, url, f"error_{str(e)[:40]}"


def run(limit=None):
    conn = get_db()

    # Find businesses due for scraping
    query = """
        SELECT id, name, website FROM businesses
        WHERE website IS NOT NULL AND website != ''
        AND (
            html_fetched_at IS NULL
            OR html_fetched_at < datetime('now', '-{} days')
        )
        ORDER BY html_fetched_at ASC NULLS FIRST
    """.format(STALE_DAYS)

    if limit:
        query += f" LIMIT {limit}"

    due = conn.execute(query).fetchall()
    total_due = len(due)

    if total_due == 0:
        print("All websites are fresh. Nothing to scrape.")
        conn.close()
        return

    print(f"Scraping {total_due} websites (stale > {STALE_DAYS} days)...")
    log_id = conn.execute(
        "INSERT INTO pipeline_log (stage) VALUES ('scrape_html')"
    ).lastrowid
    conn.commit()

    ok = dead = errors = 0

    for i, row in enumerate(due):
        biz_id = row["id"]
        name = row["name"]
        url = row["website"]

        print(f"  [{i+1}/{total_due}] {name[:40]:<40} {url[:50]}", end=" ", flush=True)

        html, final_url, status = fetch_html(url)

        if html:
            # Trim to 500KB max to keep DB manageable
            if len(html) > 500_000:
                html = html[:500_000]
            conn.execute("""
                UPDATE businesses SET
                    html_content=?, html_fetched_at=datetime('now'),
                    website_status=?, website=?
                WHERE id=?
            """, (html, status, final_url, biz_id))
            ok += 1
            print(f"✓ ({len(html)//1024}KB)")
        else:
            conn.execute("""
                UPDATE businesses SET
                    html_fetched_at=datetime('now'), website_status=?
                WHERE id=?
            """, (status, biz_id))
            dead += 1
            print(f"✗ {status}")

        # Commit every 10 records
        if (i + 1) % 10 == 0:
            conn.commit()

        time.sleep(DELAY)

    conn.commit()
    conn.execute("""
        UPDATE pipeline_log SET finished_at=datetime('now'), status='ok',
        records_processed=?, records_changed=?,
        message=?
        WHERE id=?
    """, (total_due, ok, f"ok:{ok} dead:{dead} errors:{errors}", log_id))
    conn.commit()

    print(f"\nDone. OK: {ok}  Dead: {dead}  Errors: {errors}")
    conn.close()


if __name__ == "__main__":
    # Optional: pass a limit as first arg for testing
    limit = int(sys.argv[1]) if len(sys.argv) > 1 else None
    run(limit)
