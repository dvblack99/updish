#!/usr/bin/env python3
"""
enrich_urls.py — CaféCompanion Phase 0
Reads a café CSV, searches Google for missing website URLs,
writes an enriched CSV. Run on your VPS.

Usage:
    python3 enrich_urls.py input.csv output.csv

Requirements:
    pip3 install requests beautifulsoup4 --break-system-packages
"""

import csv
import sys
import time
import random
import logging
import re
from pathlib import Path

import requests
from bs4 import BeautifulSoup

# ─── Config ───────────────────────────────────────────────────────────────────

# Domains we reject as "not the café's own site"
SKIP_DOMAINS = {
    'yelp.com', 'tripadvisor.com', 'tripadvisor.ca',
    'google.com', 'google.ca', 'maps.google',
    'facebook.com', 'instagram.com', 'twitter.com', 'x.com',
    'zomato.com', 'opentable.com', 'doordash.com', 'ubereats.com',
    'skipthedishes.com', 'grubhub.com', 'seamless.com',
    'foursquare.com', 'yellowpages.ca', 'yellowpages.com',
    'canada411.ca', 'yelp.ca', 'blogto.com', 'vancouverisawesome.com',
    'reddit.com', 'wikipedia.org', 'wikimedia.org',
    'bbb.org', 'groupon.com', 'expedia.com',
}

# Pause between requests: random value between these two (seconds)
PAUSE_MIN = 3.0
PAUSE_MAX = 6.0

# Rotate user agents to be polite
USER_AGENTS = [
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36',
    'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Safari/605.1.15',
]

# ─── Logging ──────────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s  %(levelname)-7s  %(message)s',
    datefmt='%H:%M:%S',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('enrich_urls.log', encoding='utf-8'),
    ]
)
log = logging.getLogger(__name__)

# ─── URL helpers ──────────────────────────────────────────────────────────────

def is_skip_domain(url: str) -> bool:
    url_lower = url.lower()
    return any(skip in url_lower for skip in SKIP_DOMAINS)


def clean_url(url: str) -> str:
    """Strip protocol and trailing slash for storage."""
    url = re.sub(r'^https?://', '', url)
    url = url.rstrip('/')
    return url


def extract_urls_from_google(html: str) -> list[str]:
    """Parse Google search result HTML and return ranked list of result URLs."""
    soup = BeautifulSoup(html, 'html.parser')
    urls = []

    # Google wraps organic results in <a> tags with /url?q= hrefs
    for a in soup.find_all('a', href=True):
        href = a['href']
        if href.startswith('/url?q='):
            # Extract the actual URL
            actual = href[7:]  # strip /url?q=
            actual = actual.split('&')[0]  # strip tracking params
            if actual.startswith('http') and 'google.com' not in actual:
                urls.append(actual)

    return urls


def find_cafe_url(name: str, city: str, session: requests.Session) -> str:
    """Search Google for a café's website. Returns clean URL or empty string."""
    query = f'"{name}" {city} cafe restaurant official website'
    search_url = f'https://www.google.com/search?q={requests.utils.quote(query)}&num=5'

    headers = {
        'User-Agent': random.choice(USER_AGENTS),
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        'Accept-Language': 'en-CA,en;q=0.9',
        'Accept-Encoding': 'gzip, deflate, br',
        'DNT': '1',
        'Connection': 'keep-alive',
    }

    try:
        resp = session.get(search_url, headers=headers, timeout=10)
        if resp.status_code == 429:
            log.warning('Rate limited by Google — sleeping 60s')
            time.sleep(60)
            return ''
        if resp.status_code != 200:
            log.warning(f'Google returned {resp.status_code} for: {name}')
            return ''

        urls = extract_urls_from_google(resp.text)

        for url in urls:
            if not is_skip_domain(url):
                return clean_url(url)

    except requests.RequestException as e:
        log.error(f'Request failed for "{name}": {e}')

    return ''

# ─── CSV processing ───────────────────────────────────────────────────────────

def load_csv(path: str) -> tuple[list[dict], list[str]]:
    """Load CSV, return (rows, fieldnames)."""
    rows = []
    with open(path, newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        fieldnames = reader.fieldnames or []
        for row in reader:
            rows.append(dict(row))
    return rows, list(fieldnames)


def save_csv(path: str, rows: list[dict], fieldnames: list[str]):
    """Save rows to CSV."""
    with open(path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


# ─── Main ─────────────────────────────────────────────────────────────────────

def main():
    if len(sys.argv) < 3:
        print('Usage: python3 enrich_urls.py input.csv output.csv')
        sys.exit(1)

    input_path = sys.argv[1]
    output_path = sys.argv[2]

    if not Path(input_path).exists():
        log.error(f'Input file not found: {input_path}')
        sys.exit(1)

    log.info(f'Loading {input_path}')
    rows, fieldnames = load_csv(input_path)

    # Ensure 'website' column exists
    if 'website' not in fieldnames:
        fieldnames.append('website')
        for row in rows:
            row['website'] = ''

    # Count what needs enriching
    needs_url = [r for r in rows if not r.get('website', '').strip()]
    already_has = len(rows) - len(needs_url)

    log.info(f'Total rows:      {len(rows)}')
    log.info(f'Already have URL:{already_has}')
    log.info(f'Need enrichment: {len(needs_url)}')

    if not needs_url:
        log.info('Nothing to enrich — all rows already have URLs.')
        save_csv(output_path, rows, fieldnames)
        return

    # Estimate time
    avg_pause = (PAUSE_MIN + PAUSE_MAX) / 2
    est_minutes = round((len(needs_url) * avg_pause) / 60)
    log.info(f'Estimated time:  ~{est_minutes} minutes')
    log.info('Starting enrichment...')
    log.info('─' * 50)

    session = requests.Session()
    found = 0
    processed = 0

    for row in rows:
        if row.get('website', '').strip():
            continue  # already has URL, skip

        name = row.get('name', '').strip()
        city = row.get('city', 'Vancouver').strip()

        if not name:
            continue

        processed += 1
        url = find_cafe_url(name, city, session)

        if url:
            row['website'] = url
            found += 1
            log.info(f'[{processed}/{len(needs_url)}] ✓  {name} → {url}')
        else:
            log.info(f'[{processed}/{len(needs_url)}] —  {name} (not found)')

        # Save progress every 25 rows so you don't lose work if interrupted
        if processed % 25 == 0:
            save_csv(output_path, rows, fieldnames)
            log.info(f'    Progress saved → {output_path}')

        # Polite pause
        time.sleep(random.uniform(PAUSE_MIN, PAUSE_MAX))

    # Final save
    save_csv(output_path, rows, fieldnames)

    log.info('─' * 50)
    log.info(f'Done. {found}/{len(needs_url)} URLs found ({round(found/len(needs_url)*100) if needs_url else 0}%)')
    log.info(f'Output: {output_path}')
    log.info(f'Log:    enrich_urls.log')


if __name__ == '__main__':
    main()
