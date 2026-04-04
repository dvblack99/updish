#!/usr/bin/env python3
"""
fetch_cafes.py — CaféCompanion Phase 0
Fetches all cafés and restaurants in the Lower Mainland from OpenStreetMap
using small tiles to avoid timeouts. Saves progress as it goes.

Usage:
    python3 fetch_cafes.py

Output:
    cafes_raw.csv — all venues found

Requirements:
    pip3 install requests --break-system-packages
"""

import csv
import time
import random
import logging
import json
import sys
from pathlib import Path

import requests

# ─── Config ───────────────────────────────────────────────────────────────────

OUTPUT_FILE = 'cafes_raw.csv'
PROGRESS_FILE = 'fetch_progress.json'

# Lower Mainland bounding box
REGION = { 'south': 49.00, 'west': -123.30, 'north': 49.45, 'east': -122.40 }

# Tile size — small enough to never timeout
LAT_STEP = 0.04
LON_STEP = 0.07

# Pause between tiles (seconds) — be polite to Overpass
PAUSE_MIN = 5
PAUSE_MAX = 9

# Retry settings
MAX_RETRIES = 4
RETRY_PAUSE = 15

# Overpass mirrors — rotated on each retry
OVERPASS_MIRRORS = [
    'https://overpass-api.de/api/interpreter',
    'https://overpass.kumi.systems/api/interpreter',
    'https://maps.mail.ru/osm/tools/overpass/api/interpreter',
    'https://overpass.openstreetmap.ru/api/interpreter',
]

# ─── Logging ──────────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s  %(message)s',
    datefmt='%H:%M:%S',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('fetch_cafes.log', encoding='utf-8'),
    ]
)
log = logging.getLogger(__name__)

# ─── Tile builder ─────────────────────────────────────────────────────────────

def build_tiles():
    tiles = []
    lat = REGION['south']
    while lat < REGION['north']:
        lon = REGION['west']
        next_lat = round(min(lat + LAT_STEP, REGION['north']), 5)
        while lon < REGION['east']:
            next_lon = round(min(lon + LON_STEP, REGION['east']), 5)
            tiles.append({
                'south': round(lat, 5),
                'west':  round(lon, 5),
                'north': next_lat,
                'east':  next_lon,
            })
            lon = next_lon
        lat = next_lat
    return tiles

# ─── Overpass query ───────────────────────────────────────────────────────────

def fetch_tile(tile, session):
    bbox = f"{tile['south']},{tile['west']},{tile['north']},{tile['east']}"
    query = f"""[out:json][timeout:25];
(
  node["amenity"~"cafe|restaurant"]({bbox});
  way["amenity"~"cafe|restaurant"]({bbox});
);
out center tags;"""

    for attempt in range(MAX_RETRIES):
        mirror = OVERPASS_MIRRORS[attempt % len(OVERPASS_MIRRORS)]
        try:
            resp = session.post(mirror, data={'data': query}, timeout=35)
            if resp.status_code == 200:
                return resp.json().get('elements', [])
            elif resp.status_code in (429, 504):
                log.warning(f'  HTTP {resp.status_code} on {mirror} — trying next mirror in {RETRY_PAUSE}s')
                time.sleep(RETRY_PAUSE)
            else:
                log.warning(f'  HTTP {resp.status_code} on {mirror} — skipping tile')
                return []
        except requests.RequestException as e:
            log.warning(f'  Error on {mirror}: {e} — trying next mirror in {RETRY_PAUSE}s')
            time.sleep(RETRY_PAUSE)

    log.warning('  All mirrors failed — skipping tile')
    return []

# ─── Clean elements ───────────────────────────────────────────────────────────

def guess_city(lat, lon):
    if lat > 49.38: return 'North Vancouver'
    if lon < -123.10 and lat > 49.25: return 'Vancouver'
    if lat < 49.18 and lon < -123.05: return 'Richmond'
    if lat > 49.22 and lon > -122.90: return 'Burnaby'
    if lat < 49.20 and lon > -122.90: return 'Surrey'
    if lon > -122.70: return 'Langley'
    return 'Vancouver'

def clean_element(el):
    tags = el.get('tags', {})
    name = tags.get('name', '').strip()
    if not name:
        return None

    lat = el.get('lat') or (el.get('center') or {}).get('lat')
    lon = el.get('lon') or (el.get('center') or {}).get('lon')
    if not lat or not lon:
        return None

    city = (tags.get('addr:city') or tags.get('is_in:city') or
            tags.get('city') or guess_city(lat, lon))

    website = (tags.get('website') or tags.get('contact:website') or '')
    website = website.replace('https://', '').replace('http://', '').rstrip('/')

    return {
        'name':    name,
        'city':    city,
        'type':    tags.get('amenity', 'cafe'),
        'address': ' '.join(filter(None, [tags.get('addr:housenumber'), tags.get('addr:street')])),
        'lat':     f'{lat:.5f}',
        'lon':     f'{lon:.5f}',
        'phone':   tags.get('phone') or tags.get('contact:phone') or '',
        'website': website,
        'source':  'OSM',
        'osm_id':  str(el.get('id', '')),
    }

# ─── Progress helpers ─────────────────────────────────────────────────────────

def load_progress():
    if Path(PROGRESS_FILE).exists():
        with open(PROGRESS_FILE) as f:
            return json.load(f)
    return {'completed_tiles': [], 'total_found': 0}

def save_progress(progress):
    with open(PROGRESS_FILE, 'w') as f:
        json.dump(progress, f)

# ─── CSV helpers ──────────────────────────────────────────────────────────────

FIELDS = ['name', 'city', 'type', 'address', 'lat', 'lon', 'phone', 'website', 'source']

def append_to_csv(rows):
    file_exists = Path(OUTPUT_FILE).exists()
    with open(OUTPUT_FILE, 'a', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=FIELDS, extrasaction='ignore')
        if not file_exists:
            writer.writeheader()
        writer.writerows(rows)

def deduplicate_csv():
    log.info('Deduplicating final CSV...')
    rows = []
    seen = set()
    with open(OUTPUT_FILE, newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            # Dedup key: name + rounded lat/lon
            try:
                key = f"{row['name'].lower()}|{round(float(row['lat']),2)}|{round(float(row['lon']),2)}"
            except ValueError:
                key = row['name'].lower()
            if key not in seen:
                seen.add(key)
                rows.append(row)

    with open(OUTPUT_FILE, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=FIELDS, extrasaction='ignore')
        writer.writeheader()
        writer.writerows(rows)

    return len(rows)

# ─── Main ─────────────────────────────────────────────────────────────────────

def main():
    tiles = build_tiles()
    progress = load_progress()
    completed = set(progress['completed_tiles'])

    remaining = [t for i, t in enumerate(tiles) if i not in completed]
    total = len(tiles)
    done = len(completed)

    log.info(f'Lower Mainland tiled into {total} tiles')
    log.info(f'Already completed: {done} tiles')
    log.info(f'Remaining: {len(remaining)} tiles')
    log.info(f'Output: {OUTPUT_FILE}')
    log.info('─' * 50)

    if not remaining:
        log.info('All tiles already fetched! Running deduplication...')
        final_count = deduplicate_csv()
        log.info(f'Done — {final_count} unique venues in {OUTPUT_FILE}')
        return

    session = requests.Session()
    session.headers.update({'User-Agent': 'CafeCompanion-DataCollection/1.0'})

    total_found = progress['total_found']

    for i, tile in enumerate(remaining):
        tile_index = tiles.index(tile)
        global_num = done + i + 1

        log.info(f'Tile {global_num}/{total}  bbox={tile["south"]},{tile["west"]},{tile["north"]},{tile["east"]}')

        elements = fetch_tile(tile, session)
        cleaned = [r for r in (clean_element(e) for e in elements) if r]

        if cleaned:
            append_to_csv(cleaned)
            total_found += len(cleaned)
            log.info(f'  ✓ {len(cleaned)} venues  (running total: {total_found})')
        else:
            log.info(f'  — 0 venues')

        # Save progress
        completed.add(tile_index)
        progress['completed_tiles'] = list(completed)
        progress['total_found'] = total_found
        save_progress(progress)

        # Pause between tiles
        if i < len(remaining) - 1:
            pause = random.uniform(PAUSE_MIN, PAUSE_MAX)
            time.sleep(pause)

    log.info('─' * 50)
    log.info('All tiles fetched. Running deduplication...')
    final_count = deduplicate_csv()

    # Clean up progress file
    Path(PROGRESS_FILE).unlink(missing_ok=True)

    log.info(f'Done — {final_count} unique venues saved to {OUTPUT_FILE}')
    log.info('Next step: run enrich_urls.py on this file')


if __name__ == '__main__':
    main()
