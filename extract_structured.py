#!/usr/bin/env python3
"""
extract_structured.py
Reads raw HTML from DB, sends to xAI API to extract structured JSON,
stores result back in DB. Run after scrape_html.py.

Adds columns to businesses table:
  structured_data  TEXT  -- JSON with menu_items, hours, price_range etc
  structured_at    TEXT  -- timestamp of last extraction
"""

import sqlite3
import urllib.request
import urllib.error
import json
import time
import os
import re
import sys

DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "updish.db")
PROXY_URL = "http://localhost:8181/v1/chat/completions"
MODEL = "grok-3-fast-beta"
STALE_DAYS = 30

SYSTEM_PROMPT = """You are a data extraction assistant. Given raw HTML from a restaurant or café website, extract structured information and return ONLY valid JSON with no other text, preamble, or markdown.

Return this exact structure (use null for missing fields):
{
  "description": "1-2 sentence description of the business",
  "cuisines": ["cuisine1", "cuisine2"],
  "price_range": "$" | "$$" | "$$$" | "$$$$" | null,
  "highlights": ["feature1", "feature2"],
  "hours": {"mon":"11am-9pm", "tue":"11am-9pm", ...} or null,
  "phone": "phone number or null",
  "menu_items": [
    {"name": "item name", "price": "$12", "category": "mains", "description": "brief desc"}
  ],
  "social": {"instagram": "url", "facebook": "url"},
  "reservations": true | false | null,
  "delivery": true | false | null,
  "takeout": true | false | null,
  "outdoor_seating": true | false | null
}

Keep menu_items to max 20 most notable items. Focus on food/drink items with prices if available. Return ONLY the JSON object."""


def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    return conn


def ensure_columns(conn):
    cols = [r[1] for r in conn.execute("PRAGMA table_info(businesses)").fetchall()]
    if "structured_data" not in cols:
        conn.execute("ALTER TABLE businesses ADD COLUMN structured_data TEXT")
    if "structured_at" not in cols:
        conn.execute("ALTER TABLE businesses ADD COLUMN structured_at TEXT")
    conn.commit()


def clean_html(html):
    """Strip HTML down to meaningful text content, max 8000 chars."""
    # Remove script, style, nav, footer, header tags and their content
    html = re.sub(r'<(script|style|nav|footer|header|noscript)[^>]*>.*?</\1>', ' ', html, flags=re.DOTALL|re.IGNORECASE)
    # Remove HTML comments
    html = re.sub(r'<!--.*?-->', ' ', html, flags=re.DOTALL)
    # Remove remaining tags
    html = re.sub(r'<[^>]+>', ' ', html)
    # Decode common entities
    html = html.replace('&amp;', '&').replace('&lt;', '<').replace('&gt;', '>').replace('&nbsp;', ' ').replace('&#39;', "'").replace('&quot;', '"')
    # Collapse whitespace
    html = re.sub(r'\s+', ' ', html).strip()
    # Truncate
    return html[:8000]


def call_xai(text, name):
    """Call xAI via proxy, return parsed JSON or None."""
    payload = json.dumps({
        "model": MODEL,
        "max_tokens": 1000,
        "temperature": 0.1,
        "messages": [
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": f"Business name: {name}\n\nWebsite text:\n{text}"}
        ]
    }).encode()

    req = urllib.request.Request(
        PROXY_URL,
        data=payload,
        headers={
            "Content-Type": "application/json",
            "X-Token": os.environ.get("UPDISH_TOKEN", "updish"),
        }
    )

    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            data = json.loads(resp.read().decode())
            content = data["choices"][0]["message"]["content"].strip()
            # Strip markdown fences if present
            content = re.sub(r'^```json\s*', '', content)
            content = re.sub(r'\s*```$', '', content)
            return json.loads(content)
    except (urllib.error.URLError, KeyError, json.JSONDecodeError) as e:
        return None


def run(limit=None):
    conn = get_db()
    ensure_columns(conn)

    query = """
        SELECT id, name, html_content, website FROM businesses
        WHERE html_content IS NOT NULL
        AND html_content != ''
        AND website_status = 'ok'
        AND (
            structured_at IS NULL
            OR structured_at < datetime('now', '-{} days')
        )
        ORDER BY structured_at ASC NULLS FIRST
    """.format(STALE_DAYS)

    if limit:
        query += f" LIMIT {limit}"

    due = conn.execute(query).fetchall()
    total = len(due)

    if total == 0:
        print("Nothing to extract.")
        conn.close()
        return

    print(f"Extracting structured data for {total} businesses...")
    log_id = conn.execute(
        "INSERT INTO pipeline_log (stage) VALUES ('extract_structured')"
    ).lastrowid
    conn.commit()

    ok = failed = 0

    for i, row in enumerate(due):
        name = row["name"]
        html = row["html_content"]
        print(f"  [{i+1}/{total}] {name[:50]:<50}", end=" ", flush=True)

        cleaned = clean_html(html)
        if len(cleaned) < 100:
            print("skip (too little content)")
            continue

        result = call_xai(cleaned, name)

        if result:
            conn.execute("""
                UPDATE businesses SET
                    structured_data=?, structured_at=datetime('now')
                WHERE id=?
            """, (json.dumps(result, ensure_ascii=False), row["id"]))
            ok += 1
            price = result.get("price_range") or "?"
            cuisines = ", ".join(result.get("cuisines") or [])[:30]
            items = len(result.get("menu_items") or [])
            print(f"✓ {price} {cuisines} ({items} menu items)")
        else:
            failed += 1
            print("✗ extraction failed")

        if (i + 1) % 10 == 0:
            conn.commit()

        time.sleep(0.5)

    conn.commit()
    conn.execute("""
        UPDATE pipeline_log SET finished_at=datetime('now'), status='ok',
        records_processed=?, records_changed=?, message=?
        WHERE id=?
    """, (total, ok, f"ok:{ok} failed:{failed}", log_id))
    conn.commit()

    print(f"\nDone. Extracted: {ok}  Failed: {failed}")
    conn.close()


if __name__ == "__main__":
    limit = int(sys.argv[1]) if len(sys.argv) > 1 else None
    run(limit)
