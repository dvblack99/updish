#!/usr/bin/env python3
"""
build_db.py
Creates updish.db and imports CoV licence data + merges OSM websites.
Run once to initialize, safe to re-run (uses upsert).
"""

import sqlite3
import urllib.request
import urllib.parse
import json
import csv
import time
import os
import re

DB_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "updish.db")
OSM_CSV = os.path.join(os.path.dirname(os.path.abspath(__file__)), "data/latest_cafes.csv")
API_BASE = "https://opendata.vancouver.ca/api/explore/v2.1/catalog/datasets/business-licences/records"
FOOD_TYPES = ["Restaurant", "Liquor Establishment", "Street Vendor", "Caterer"]
LIQUOR_SUBTYPES = {"Class 1 with liquor service", "Class 2 with liquor service"}


def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    return conn


def init_schema(conn):
    conn.executescript("""
    CREATE TABLE IF NOT EXISTS businesses (
        id              INTEGER PRIMARY KEY AUTOINCREMENT,
        licence_rsn     TEXT UNIQUE,           -- CoV licence RSN (stable unique key)
        name            TEXT NOT NULL,
        address         TEXT,
        neighbourhood   TEXT,
        city            TEXT DEFAULT 'Vancouver',
        postal_code     TEXT,
        type            TEXT,                  -- restaurant / restaurant_liquor / bar / street_vendor / caterer
        licence_type    TEXT,                  -- Food Primary / Liquor Primary etc
        employees       INTEGER DEFAULT 0,
        lat             REAL,
        lng             REAL,
        website         TEXT,                  -- URL if known
        website_source  TEXT,                  -- osm / manual / scraped
        html_content    TEXT,                  -- raw HTML from website
        html_fetched_at TEXT,                  -- ISO timestamp of last fetch
        website_status  TEXT DEFAULT 'unknown', -- unknown / ok / dead / redirected
        licence_status  TEXT DEFAULT 'active',
        first_seen      TEXT DEFAULT (datetime('now')),
        last_verified   TEXT DEFAULT (datetime('now'))
    );

    CREATE TABLE IF NOT EXISTS pipeline_log (
        id          INTEGER PRIMARY KEY AUTOINCREMENT,
        stage       TEXT NOT NULL,
        started_at  TEXT DEFAULT (datetime('now')),
        finished_at TEXT,
        records_processed INTEGER DEFAULT 0,
        records_changed   INTEGER DEFAULT 0,
        status      TEXT DEFAULT 'running',   -- running / ok / error
        message     TEXT
    );

    CREATE INDEX IF NOT EXISTS idx_neighbourhood ON businesses(neighbourhood);
    CREATE INDEX IF NOT EXISTS idx_type ON businesses(type);
    CREATE INDEX IF NOT EXISTS idx_website ON businesses(website);
    CREATE INDEX IF NOT EXISTS idx_html_fetched ON businesses(html_fetched_at);
    """)
    conn.commit()
    print("Schema ready.")


def classify(bt, bs):
    if bt == "Restaurant":
        return ("restaurant_liquor", "Food Primary + Liquor") if bs in LIQUOR_SUBTYPES else ("restaurant", "Food Primary")
    elif bt == "Liquor Establishment":
        return ("bar", "Liquor Primary")
    elif bt == "Street Vendor":
        return ("street_vendor", "Street Vendor")
    elif bt == "Caterer":
        return ("caterer", "Caterer")
    return ("restaurant", "Food Primary")


def fetch_cov_page(offset, limit=100):
    quoted = [f"'{t}'" for t in FOOD_TYPES]
    where = f"businesstype in ({', '.join(quoted)}) AND status='Issued'"
    params = (
        f"where={urllib.parse.quote(where)}"
        f"&limit={limit}&offset={offset}"
        f"&fields=licencersn,businessname,businesstradename,businesstype,businesssubtype,"
        f"status,numberofemployees,house,street,localarea,postalcode,geo_point_2d"
    )
    req = urllib.request.Request(f"{API_BASE}?{params}", headers={"User-Agent": "UpDish/1.0"})
    with urllib.request.urlopen(req, timeout=30) as resp:
        return json.loads(resp.read().decode())


def import_cov(conn):
    print("Importing CoV licence data...")
    log_id = conn.execute(
        "INSERT INTO pipeline_log (stage) VALUES ('sync_licences')"
    ).lastrowid
    conn.commit()

    offset, limit, total = 0, 100, None
    processed = changed = 0

    while True:
        data = fetch_cov_page(offset, limit)
        if total is None:
            total = data.get("total_count", 0)
            print(f"  Total records: {total}")

        results = data.get("results", [])
        if not results:
            break

        for r in results:
            geo = r.get("geo_point_2d")
            if not geo:
                continue
            lat, lon = geo.get("lat"), geo.get("lon")
            if not lat or not lon:
                continue

            rsn = r.get("licencersn", "")
            name = r.get("businesstradename") or r.get("businessname") or "Unknown"
            address = " ".join(filter(None, [r.get("house", ""), r.get("street", "")])).strip()
            neighbourhood = r.get("localarea") or "Vancouver"
            postal = r.get("postalcode", "")
            employees = int(r.get("numberofemployees") or 0)
            type_key, licence = classify(r.get("businesstype", ""), r.get("businesssubtype", ""))

            existing = conn.execute(
                "SELECT id, name, address, lat, lng FROM businesses WHERE licence_rsn=?", (rsn,)
            ).fetchone()

            if existing:
                conn.execute("""
                    UPDATE businesses SET
                        name=?, address=?, neighbourhood=?, postal_code=?,
                        type=?, licence_type=?, employees=?, lat=?, lng=?,
                        licence_status='active', last_verified=datetime('now')
                    WHERE licence_rsn=?
                """, (name, address, neighbourhood, postal, type_key, licence, employees, lat, lon, rsn))
                changed += 1
            else:
                conn.execute("""
                    INSERT INTO businesses
                        (licence_rsn, name, address, neighbourhood, postal_code,
                         type, licence_type, employees, lat, lng, licence_status)
                    VALUES (?,?,?,?,?,?,?,?,?,?,'active')
                """, (rsn, name, address, neighbourhood, postal, type_key, licence, employees, lat, lon))
                changed += 1

            processed += 1

        offset += limit
        if offset >= total:
            break

        if offset % 500 == 0:
            conn.commit()
            print(f"  {offset}/{total}...", end="\r")
        time.sleep(0.15)

    conn.commit()
    conn.execute("""
        UPDATE pipeline_log SET finished_at=datetime('now'), status='ok',
        records_processed=?, records_changed=?, message='CoV import complete'
        WHERE id=?
    """, (processed, changed, log_id))
    conn.commit()
    print(f"\n  Imported {processed} records ({changed} new/updated).")


def merge_osm_websites(conn):
    if not os.path.exists(OSM_CSV):
        print("No OSM CSV found, skipping website merge.")
        return

    print("Merging OSM websites...")
    log_id = conn.execute(
        "INSERT INTO pipeline_log (stage) VALUES ('merge_osm')"
    ).lastrowid
    conn.commit()

    with open(OSM_CSV, encoding="utf-8") as f:
        osm_rows = list(csv.DictReader(f))

    # Build OSM lookup: normalized name -> website
    osm_map = {}
    for row in osm_rows:
        url = row.get("website", "").strip()
        if not url:
            continue
        key = normalize_name(row.get("name", ""))
        if key:
            osm_map[key] = url

    matched = 0
    businesses = conn.execute(
        "SELECT id, name FROM businesses WHERE website IS NULL OR website=''"
    ).fetchall()

    for b in businesses:
        key = normalize_name(b["name"])
        if key in osm_map:
            conn.execute(
                "UPDATE businesses SET website=?, website_source='osm' WHERE id=?",
                (osm_map[key], b["id"])
            )
            matched += 1

    conn.commit()
    conn.execute("""
        UPDATE pipeline_log SET finished_at=datetime('now'), status='ok',
        records_processed=?, records_changed=?, message='OSM merge complete'
        WHERE id=?
    """, (len(businesses), matched, log_id))
    conn.commit()
    print(f"  Matched {matched} websites from OSM data.")


def normalize_name(name):
    """Lowercase, strip punctuation and common words for fuzzy matching."""
    name = name.lower().strip()
    name = re.sub(r"[''\".,!&]", "", name)
    name = re.sub(r"\b(the|a|an|ltd|inc|co|restaurant|cafe|coffee)\b", "", name)
    name = re.sub(r"\s+", " ", name).strip()
    return name


def print_summary(conn):
    total = conn.execute("SELECT COUNT(*) FROM businesses").fetchone()[0]
    with_web = conn.execute("SELECT COUNT(*) FROM businesses WHERE website IS NOT NULL AND website != ''").fetchone()[0]
    by_type = conn.execute("SELECT type, COUNT(*) as c FROM businesses GROUP BY type ORDER BY c DESC").fetchall()
    print(f"\nDatabase summary:")
    print(f"  Total businesses: {total}")
    print(f"  With websites:    {with_web} ({100*with_web//total}%)")
    print(f"  By type:")
    for row in by_type:
        print(f"    {row['type']}: {row['c']}")


if __name__ == "__main__":
    conn = get_db()
    init_schema(conn)
    import_cov(conn)
    merge_osm_websites(conn)
    print_summary(conn)
    conn.close()
    print("\nDone. Run export_data_js.py to regenerate the map data.")
