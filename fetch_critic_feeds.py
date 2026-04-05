#!/usr/bin/env python3
"""
fetch_critic_feeds.py — Updish daily/monthly/weekly data fetcher

Schedule:
  Daily (every run):   RSS feeds, Google News, venue news queries, outlink discovery
  Weekly (day 1,8,15,22,29): scrape critic article pages, fetch venue photos
  Monthly (day 1):     scrape library URLs, check for updates

Crontab:
  0 3 * * * /usr/bin/python3 /root/mysite/html/updish/scripts/fetch_critic_feeds.py >> /root/mysite/html/updish/scripts/fetch_critic_feeds.log 2>&1

Requirements:
  pip3 install requests feedparser beautifulsoup4 pdfminer.six --break-system-packages
"""

import json, time, logging, hashlib, sys, re, csv, random
from pathlib import Path
from datetime import datetime, timezone
from urllib.parse import urlparse, urljoin
from collections import Counter
import requests, feedparser
from bs4 import BeautifulSoup

# ─── Paths ────────────────────────────────────────────────────────────────────

BASE             = Path('/root/mysite/html/updish')
DATA_DIR         = BASE / 'data'
SCRIPTS_DIR      = BASE / 'scripts'
LIBRARY_DIR      = DATA_DIR / 'library'
CRITIC_DIR       = DATA_DIR / 'critic_scrapes'
VENUE_DIR        = DATA_DIR / 'venues'

OUTPUT_JSON          = DATA_DIR / 'critic_feed.json'
LIBRARY_STATUS_JSON  = DATA_DIR / 'library_status.json'
CANDIDATES_JSON      = DATA_DIR / 'candidate_sources.json'
VENUE_STATUS_JSON    = DATA_DIR / 'venue_status.json'
LOG_FILE             = SCRIPTS_DIR / 'fetch_critic_feeds.log'

for d in [DATA_DIR, SCRIPTS_DIR, LIBRARY_DIR, CRITIC_DIR, VENUE_DIR]:
    d.mkdir(parents=True, exist_ok=True)

# ─── Config ───────────────────────────────────────────────────────────────────

MAX_ENTRIES      = 500
VENUE_SAMPLE     = 8
REQUEST_TIMEOUT  = 20
VENUES_PER_RUN   = 60       # ~60/day × 7 days = 420/week, full pass ~3 weeks
MIN_TEXT_LENGTH  = 200      # skip scrapes returning less than this (paywalls)
MAPILLARY_TOKEN  = 'MLY|25661890350153232|db6e082fdab9fd44630d8153831e9498'

SKIP_DOMAINS = {
    'google.com','google.ca','facebook.com','instagram.com','twitter.com','x.com',
    'youtube.com','linkedin.com','apple.com','amazon.ca','amazon.com',
    'wikipedia.org','wikimedia.org','yelp.com','yelp.ca','tripadvisor.com',
    'tripadvisor.ca','doordash.com','ubereats.com','skipthedishes.com',
    'opentable.com','zomato.com','foursquare.com',
    'gov.bc.ca','gc.ca','statcan.gc.ca','bclaws.gov.bc.ca','dal.ca','ubc.ca','sfu.ca',
    'scoutmagazine.ca','vancouver.eater.com','vancouversun.com','straight.com',
    'miss604.com','vancouverisawesome.com','dailyhive.com','vaneats.ca',
    'reddit.com','news.google.com',
}

RSS_SOURCES = [
    {'id':'scout',         'name':'Scout Magazine',        'url':'https://scoutmagazine.ca/feed/',                      'focus':'Independent dining, cafes, openings',      'color':'#e8834a'},
    {'id':'eater_van',     'name':'Eater Vancouver',       'url':'https://vancouver.eater.com/rss/index.xml',           'focus':'Openings, closures, trends',               'color':'#e05c5c'},
    {'id':'van_sun_food',  'name':'Vancouver Sun Food',    'url':'https://vancouversun.com/category/life/food/feed',    'focus':'Reviews, features, industry news',          'color':'#4a7be8'},
    {'id':'straight_food', 'name':'Georgia Straight Food', 'url':'https://www.straight.com/food/feed',                 'focus':'Restaurant reviews, chef profiles',         'color':'#4caf79'},
    {'id':'miss604',       'name':'Miss604',               'url':'https://miss604.com/category/food/feed',              'focus':'Vancouver lifestyle, neighbourhood dining',  'color':'#c86eb5'},
    {'id':'van_is_awesome','name':'Vancouver Is Awesome',  'url':'https://www.vancouverisawesome.com/food-drink/feed',  'focus':'Local dining, openings, lists',             'color':'#d4a84b'},
    {'id':'daily_hive_van','name':'Daily Hive Vancouver',  'url':'https://dailyhive.com/vancouver/category/food/feed', 'focus':'Openings, closures, deals',                'color':'#7a5f35'},
    {'id':'van_eats',      'name':'Van Eats',              'url':'https://vaneats.ca/feed/',                           'focus':'Restaurant reviews, food photography',      'color':'#c8a96e'},
]

GOOGLE_NEWS_QUERIES = [
    'Vancouver restaurant opening',
    'Vancouver cafe new',
    'Lower Mainland restaurant review',
    'North Vancouver restaurant',
    'Burnaby Surrey restaurant',
]

REDDIT_SUBS = ['FoodVancouver', 'vancouver']
REDDIT_FOOD_KEYWORDS = [
    'restaurant','cafe','coffee','brunch','ramen','sushi','pizza','burger',
    'dim sum','pho','opened','opening','closed','closing','review','recommend',
    'hidden gem','best','underrated','overrated','menu','chef','bakery',
    'pastry','dessert','bar','izakaya',
]

# Sources worth scraping full text (skip reddit/google_news)
SCRAPE_SOURCE_IDS = {'scout','eater_van','van_sun_food','straight_food','miss604','van_is_awesome','daily_hive_van','van_eats'}

LIBRARY_ENTRIES = [
    {'id':'statcan_food_dec24',    'title':'Food Services & Drinking Places Dec 2024',         'url':'https://www150.statcan.gc.ca/n1/daily-quotidien/250225/dq250225c-eng.htm','type':'html'},
    {'id':'statcan_food_ann23',    'title':'Food Services & Drinking Places Annual 2023',       'url':'https://www150.statcan.gc.ca/n1/daily-quotidien/250218/dq250218d-eng.htm','type':'html'},
    {'id':'statcan_food_dash',     'title':'Food Services Sales Dashboard',                     'url':'https://www150.statcan.gc.ca/n1/en/catalogue/71-607-X2017003',           'type':'html'},
    {'id':'aafc_foodservice',      'title':'Foodservice in Canada AAFC',                        'url':'https://agriculture.canada.ca/en/international-trade/market-intelligence/reports-and-guides/foodservice-canada','type':'html'},
    {'id':'statcan_shs23',         'title':'Survey of Household Spending 2023',                 'url':'https://www150.statcan.gc.ca/n1/daily-quotidien/250521/dq250521a-eng.htm','type':'html'},
    {'id':'cfpr_2026_pdf',         'title':"Canada's Food Price Report 2026",                   'url':'https://cdn.dal.ca/content/dam/dalhousie/pdf/sites/agri-food/FINAL%20E%20low.res%20DAL_PRICE_REPORT_2026.pdf','type':'pdf'},
    {'id':'cfpr_2025_pdf',         'title':"Canada's Food Price Report 2025",                   'url':'https://cdn.dal.ca/content/dam/dalhousie/pdf/sites/agri-food/EN%20-%20Food%20Price%20Report%202025.pdf','type':'pdf'},
    {'id':'cfpr_2024',             'title':"Canada's Food Price Report 2024",                   'url':'https://www.dal.ca/sites/agri-food/research/canada-s-food-price-report-2024.html','type':'html'},
    {'id':'statcan_food_price_hub','title':'StatsCan Food Price Data Hub',                      'url':'https://www.statcan.gc.ca/en/topics-start/food-price',                  'type':'html'},
    {'id':'statcan_cpi_food',      'title':'CPI Food Monthly by Province',                      'url':'https://www150.statcan.gc.ca/t1/tbl1/en/tv.action?pid=1810000403',       'type':'html'},
    {'id':'bc_food_safety_leg',    'title':'BC Food Safety Legislation Overview',               'url':'https://www2.gov.bc.ca/gov/content/health/keeping-bc-healthy-safe/food-safety/food-safety-legislation','type':'html'},
    {'id':'bc_food_premises_reg',  'title':'BC Food Premises Regulation',                       'url':'https://www.bclaws.gov.bc.ca/civix/document/id/complete/statreg/210_99', 'type':'html'},
    {'id':'bccdc_food_guidelines', 'title':'Food Premises Guidelines BCCDC',                    'url':'https://www.bccdc.ca/health-professionals/professional-resources/food-premises-guidelines','type':'html'},
    {'id':'vch_food_safety',       'title':'Food Safety Resources Vancouver Coastal Health',    'url':'https://www.vch.ca/en/food-safety-resources',                           'type':'html'},
    {'id':'fraser_new_biz',        'title':'Requirements for New Food Businesses Fraser Health','url':'https://www.fraserhealth.ca/health-topics-a-to-z/food-safety/requirements-for-food-businesses','type':'html'},
    {'id':'foodsafe_resources',    'title':'FOODSAFE Program Resources',                        'url':'https://www.foodsafe.ca/resources.html',                                'type':'html'},
    {'id':'bc_ag_stats',           'title':'BC Agriculture Seafood Statistics',                 'url':'https://www2.gov.bc.ca/gov/content/industry/agriculture-seafood/statistics/agriculture-and-seafood-statistics-publications','type':'html'},
    {'id':'bc_maf',                'title':'BC Ministry of Agriculture and Food',               'url':'https://gov.bc.ca/af',                                                   'type':'html'},
    {'id':'bc_gap',                'title':'Good Agricultural Practices On-Farm',               'url':'https://www2.gov.bc.ca/gov/content/industry/agriculture-seafood/food-safety/good-agricultural-practices/scope-of-good-agricultural-practices','type':'html'},
    {'id':'bc_maf_annual_pdf',     'title':'BC Ministry of Agriculture 2024/25 Service Plan',  'url':'https://www.bcbudget.gov.bc.ca/Annual_Reports/2024_2025/pdf/ministry/af.pdf','type':'pdf'},
    {'id':'bc_food_class_act',     'title':'BC Food and Agricultural Products Classification Act','url':'https://www.bclaws.gov.bc.ca/civix/document/id/complete/statreg/16001','type':'html'},
    {'id':'van_zoning_lib',        'title':'Vancouver Zoning Land Use Document Library',        'url':'https://vancouver.ca/home-property-development/zoning-and-land-use-policies-document-library.aspx','type':'html'},
    {'id':'van_c2_pdf',            'title':'Vancouver C-2 District Schedule',                   'url':'https://bylaws.vancouver.ca/zoning/zoning-by-law-district-schedule-c-2.pdf','type':'pdf'},
    {'id':'van_zoning_bylaw',      'title':'Vancouver Zoning and Development By-law 3575',      'url':'https://vancouver.ca/home-property-development/zoning-and-development-bylaw.aspx','type':'html'},
    {'id':'ised_consumer_trends',  'title':'Consumer Trends Report Chapter 9',                  'url':'https://ised-isde.canada.ca/site/office-consumer-affairs/en/consumer-interest-groups/consumer-trends/consumer-trends-report-chapter-9-consumer-spending','type':'html'},
]

# ─── Logging ──────────────────────────────────────────────────────────────────

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s  %(levelname)-7s  %(message)s',
    datefmt='%H:%M:%S',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(str(LOG_FILE), encoding='utf-8'),
    ]
)
log = logging.getLogger(__name__)

SESSION = requests.Session()
SESSION.headers.update({'User-Agent': 'Updish-DataBot/1.0 (+https://davidjamesblack.com/updish)'})

# ─── Helpers ──────────────────────────────────────────────────────────────────

def entry_id(url, title):
    return hashlib.md5(f'{url}|{title}'.encode()).hexdigest()[:12]

def clean_html(text):
    if not text: return ''
    text = re.sub(r'<[^>]+>', ' ', text)
    return re.sub(r'\s+', ' ', text).strip()[:280]

def parse_date(entry):
    for attr in ('published_parsed','updated_parsed','created_parsed'):
        t = getattr(entry, attr, None)
        if t:
            try: return datetime(*t[:6], tzinfo=timezone.utc).isoformat()
            except: pass
    return datetime.now(timezone.utc).isoformat()

def is_food_relevant(text):
    return any(kw in text.lower() for kw in REDDIT_FOOD_KEYWORDS)

def get_domain(url):
    try: return urlparse(url).netloc.lower().lstrip('www.')
    except: return ''

def is_skip(domain):
    return any(skip in domain for skip in SKIP_DOMAINS)

def now_iso():
    return datetime.now(timezone.utc).isoformat()

def content_hash(b):
    return hashlib.md5(b).hexdigest()

def extract_text_from_html(html):
    soup = BeautifulSoup(html, 'html.parser')
    for tag in soup(['script','style','nav','footer','header']): tag.decompose()
    text = soup.get_text(separator='\n')
    return re.sub(r'\n{3,}', '\n\n', text).strip()

def extract_text_from_pdf(pdf_bytes):
    try:
        from pdfminer.high_level import extract_text_to_fp
        from pdfminer.layout import LAParams
        import io
        output = io.StringIO()
        extract_text_to_fp(io.BytesIO(pdf_bytes), output, laparams=LAParams())
        return output.getvalue().strip()
    except ImportError:
        log.warning('pdfminer.six not installed')
        return ''
    except Exception as e:
        log.warning(f'PDF extract failed: {e}'); return ''

# ─── Feed helpers ─────────────────────────────────────────────────────────────

def load_existing_feed():
    if OUTPUT_JSON.exists():
        try:
            with open(OUTPUT_JSON, encoding='utf-8') as f:
                data = json.load(f)
                return data.get('entries',[]), set(e['id'] for e in data.get('entries',[]))
        except: pass
    return [], set()

def save_feed(entries):
    with open(OUTPUT_JSON, 'w', encoding='utf-8') as f:
        json.dump({'updated':now_iso(),'count':len(entries),'entries':entries}, f, ensure_ascii=False, indent=2)
    log.info(f'Saved {len(entries)} feed entries')

# ─── RSS ──────────────────────────────────────────────────────────────────────

def fetch_rss(source, seen_ids):
    new_entries, outlinks = [], []
    try:
        feed = feedparser.parse(source['url'])
        for entry in feed.entries:
            title = clean_html(getattr(entry,'title',''))
            url   = getattr(entry,'link','')
            if not title or not url: continue
            eid = entry_id(url, title)
            if eid in seen_ids: continue
            summary = clean_html(getattr(entry,'summary','') or getattr(entry,'description','') or '')
            new_entries.append({'id':eid,'source':source['name'],'source_id':source['id'],
                'color':source['color'],'title':title,'url':url,'excerpt':summary,
                'date':parse_date(entry),'type':'article','scraped':False})
            seen_ids.add(eid)
            for href in re.findall(r'https?://[^\s"<>]+', summary):
                d = get_domain(href)
                if d and not is_skip(d): outlinks.append(d)
        log.info(f'RSS  {source["name"]:30s} -> {len(new_entries):3d} new')
    except Exception as e:
        log.warning(f'RSS {source["name"]} failed: {e}')
    return new_entries, outlinks

# ─── Google News ──────────────────────────────────────────────────────────────

def fetch_google_news(query, seen_ids):
    new_entries, outlinks = [], []
    try:
        encoded = requests.utils.quote(query)
        url = f'https://news.google.com/rss/search?q={encoded}+when:7d&hl=en-CA&gl=CA&ceid=CA:en'
        feed = feedparser.parse(url)
        for entry in feed.entries[:8]:
            title = clean_html(getattr(entry,'title',''))
            link  = getattr(entry,'link','')
            if not title or not link: continue
            eid = entry_id(link, title)
            if eid in seen_ids: continue
            new_entries.append({'id':eid,'source':'Google News','source_id':'google_news',
                'color':'#555','title':title,'url':link,'excerpt':f'Search: {query}',
                'date':parse_date(entry),'type':'news','scraped':False})
            seen_ids.add(eid)
            d = get_domain(link)
            if d and not is_skip(d): outlinks.append(d)
        log.info(f'GNews "{query[:40]}" -> {len(new_entries):3d} new')
        time.sleep(1)
    except Exception as e:
        log.warning(f'GNews "{query}" failed: {e}')
    return new_entries, outlinks

def fetch_venue_news(venue_names, seen_ids):
    new_entries, outlinks = [], []
    sample = random.sample(venue_names, min(VENUE_SAMPLE, len(venue_names)))
    for name in sample:
        e, l = fetch_google_news(f'"{name}" Vancouver restaurant cafe', seen_ids)
        new_entries.extend(e); outlinks.extend(l); time.sleep(1)
    log.info(f'Venue news: {len(new_entries)} new from {len(sample)} queries')
    return new_entries, outlinks

# ─── Reddit ───────────────────────────────────────────────────────────────────

def fetch_reddit(subreddit, seen_ids):
    new_entries, outlinks = [], []
    try:
        resp = SESSION.get(f'https://www.reddit.com/r/{subreddit}/new.json?limit=50', timeout=REQUEST_TIMEOUT)
        if resp.status_code != 200:
            log.warning(f'Reddit r/{subreddit} returned {resp.status_code}'); return [], []
        for post in resp.json().get('data',{}).get('children',[]):
            d = post.get('data',{})
            title    = d.get('title','')
            post_url = f"https://reddit.com{d.get('permalink','')}"
            text     = d.get('selftext','') or ''
            if not is_food_relevant(f'{title} {text}'): continue
            eid = entry_id(post_url, title)
            if eid in seen_ids: continue
            created = d.get('created_utc')
            date = datetime.fromtimestamp(created, tz=timezone.utc).isoformat() if created else now_iso()
            new_entries.append({'id':eid,'source':f'r/{subreddit}','source_id':f'reddit_{subreddit.lower()}',
                'color':'#ff6314','title':title,'url':post_url,'excerpt':clean_html(text),
                'date':date,'score':d.get('score',0),'type':'reddit','scraped':False})
            seen_ids.add(eid)
            for href in re.findall(r'https?://[^\s"<>]+', text):
                dom = get_domain(href)
                if dom and not is_skip(dom): outlinks.append(dom)
        log.info(f'Reddit r/{subreddit} -> {len(new_entries):3d} new')
        time.sleep(2)
    except Exception as e:
        log.warning(f'Reddit r/{subreddit} failed: {e}')
    return new_entries, outlinks

# ─── Candidate discovery ──────────────────────────────────────────────────────

def load_candidates():
    if CANDIDATES_JSON.exists():
        try:
            with open(CANDIDATES_JSON, encoding='utf-8') as f: return json.load(f)
        except: pass
    return {'candidates':[], 'promoted':[], 'dismissed':[]}

def save_candidates(data):
    with open(CANDIDATES_JSON, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

def update_candidates(all_outlinks):
    data = load_candidates()
    promoted  = set(data.get('promoted',[]))
    dismissed = set(data.get('dismissed',[]))
    known = set(get_domain(s['url']) for s in RSS_SOURCES)
    counts = Counter(all_outlinks)
    existing = {c['domain']:c for c in data.get('candidates',[])}
    for domain, count in counts.most_common(50):
        if domain in known or domain in promoted or domain in dismissed: continue
        if domain in existing:
            existing[domain]['count'] += count
            existing[domain]['last_seen'] = now_iso()
        else:
            existing[domain] = {'domain':domain,'count':count,'first_seen':now_iso(),'last_seen':now_iso()}
    sorted_c = sorted(existing.values(), key=lambda x: x['count'], reverse=True)[:100]
    data['candidates'] = sorted_c
    save_candidates(data)
    log.info(f'Candidates: {len(sorted_c)} tracked')

# ─── Generic page scraper ─────────────────────────────────────────────────────

def scrape_page(url, out_dir, entry_type='html'):
    """Download a page/PDF, save html+txt or pdf+txt. Returns status dict."""
    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    result = {'url': url, 'last_checked': now_iso(), 'error': None}
    try:
        resp = SESSION.get(url, timeout=REQUEST_TIMEOUT, allow_redirects=True)
        if resp.status_code != 200:
            result['error'] = f'HTTP {resp.status_code}'
            return result
        content  = resp.content
        new_hash = content_hash(content)
        old_hash_path = out_dir / '.hash'
        old_hash = old_hash_path.read_text().strip() if old_hash_path.exists() else ''
        changed  = new_hash != old_hash

        ct = resp.headers.get('content-type','').lower()
        is_pdf = 'pdf' in ct or str(url).endswith('.pdf')

        if is_pdf:
            (out_dir / 'document.pdf').write_bytes(content)
            txt = extract_text_from_pdf(content)
            (out_dir / 'document.txt').write_text(txt, encoding='utf-8')
            result['file'] = 'document.pdf'
            result['txt']  = 'document.txt'
        else:
            (out_dir / 'page.html').write_bytes(content)
            txt = extract_text_from_html(content.decode('utf-8', errors='replace'))
            (out_dir / 'page.txt').write_text(txt, encoding='utf-8')
            result['file'] = 'page.html'
            result['txt']  = 'page.txt'

        if len(txt) < MIN_TEXT_LENGTH and not is_pdf:
            result['error'] = 'paywall_or_empty'
            return result

        old_hash_path.write_text(new_hash)
        result.update({'hash': new_hash, 'changed': changed, 'size_bytes': len(content)})
        return result

    except Exception as e:
        result['error'] = str(e)[:120]
        return result

# ─── Critic article scraper (weekly) ─────────────────────────────────────────

def scrape_critic_articles(entries):
    """Scrape unscraped article pages from named RSS sources."""
    to_scrape = [e for e in entries
                 if not e.get('scraped') and e.get('source_id') in SCRAPE_SOURCE_IDS]
    log.info(f'Critic scrape: {len(to_scrape)} articles to scrape')
    scraped = 0
    for e in to_scrape:
        eid  = e['id']
        url  = e.get('url','')
        if not url: continue
        out_dir = CRITIC_DIR / eid
        result  = scrape_page(url, out_dir)
        e['scraped']       = True
        e['scrape_error']  = result.get('error')
        e['scrape_size']   = result.get('size_bytes', 0)
        e['scrape_changed']= result.get('changed', False)
        e['scrape_date']   = now_iso()
        scraped += 1
        time.sleep(2)
    log.info(f'Critic scrape done: {scraped} articles processed')
    return entries

# ─── Library scraper (monthly) ────────────────────────────────────────────────

def run_library_check():
    log.info('── Monthly library check ──')
    existing_status = {}
    if LIBRARY_STATUS_JSON.exists():
        try:
            with open(LIBRARY_STATUS_JSON, encoding='utf-8') as f:
                existing_status = json.load(f).get('entries', {})
        except: pass
    status = dict(existing_status)
    for entry in LIBRARY_ENTRIES:
        log.info(f'  {entry["title"][:55]}')
        out_dir = LIBRARY_DIR / entry['id']
        result  = scrape_page(entry['url'], out_dir, entry['type'])
        old     = status.get(entry['id'], {})
        status[entry['id']] = {
            'id':           entry['id'],
            'title':        entry['title'],
            'url':          entry['url'],
            'type':         entry['type'],
            'hash':         result.get('hash', old.get('hash','')),
            'changed':      result.get('changed', False),
            'last_checked': now_iso(),
            'last_changed': now_iso() if result.get('changed') else old.get('last_changed', now_iso()),
            'size_bytes':   result.get('size_bytes', 0),
            'error':        result.get('error'),
            'file':         result.get('file'),
            'txt':          result.get('txt'),
        }
    changed = sum(1 for v in status.values() if v.get('changed'))
    with open(LIBRARY_STATUS_JSON, 'w', encoding='utf-8') as f:
        json.dump({'last_checked':now_iso(),'entry_count':len(LIBRARY_ENTRIES),
                   'changed_count':changed,'entries':status}, f, ensure_ascii=False, indent=2)
    log.info(f'Library check done — {changed} changed')

# ─── Venue photo lookup (weekly) ──────────────────────────────────────────────

def fetch_mapillary_photos(lat, lon, name):
    """Return list of {thumb_url, full_url, captured_at} from Mapillary near lat/lon."""
    photos = []
    try:
        params = {
            'access_token': MAPILLARY_TOKEN,
            'fields':       'id,thumb_256_url,thumb_1024_url,captured_at',
            'bbox':         f'{float(lon)-0.002},{float(lat)-0.002},{float(lon)+0.002},{float(lat)+0.002}',
            'limit':        10,
        }
        resp = SESSION.get('https://graph.mapillary.com/images', params=params, timeout=15)
        if resp.status_code == 200:
            for img in resp.json().get('data', []):
                photos.append({
                    'source':    'Mapillary',
                    'thumb_url': img.get('thumb_256_url',''),
                    'full_url':  img.get('thumb_1024_url',''),
                    'date':      img.get('captured_at','')[:10] if img.get('captured_at') else '',
                    'type':      'exterior',
                })
    except Exception as e:
        log.warning(f'Mapillary failed for {name}: {e}')
    return photos

def fetch_wikimedia_photos(name, city):
    """Search Wikimedia Commons for venue photos."""
    photos = []
    try:
        query = f'{name} {city} restaurant cafe'
        params = {
            'action':    'query',
            'list':      'search',
            'srsearch':  f'File:{query}',
            'srnamespace': 6,
            'srlimit':   5,
            'format':    'json',
        }
        resp = SESSION.get('https://commons.wikimedia.org/w/api.php', params=params, timeout=10)
        if resp.status_code == 200:
            for item in resp.json().get('query',{}).get('search',[]):
                title = item.get('title','')
                if title:
                    fname = title.replace('File:','').replace(' ','_')
                    md5   = hashlib.md5(fname.encode()).hexdigest()
                    url   = f'https://upload.wikimedia.org/wikipedia/commons/{md5[0]}/{md5[:2]}/{fname}'
                    photos.append({
                        'source':    'Wikimedia Commons',
                        'thumb_url': url,
                        'full_url':  url,
                        'date':      '',
                        'type':      'unknown',
                        'title':     title,
                    })
    except Exception as e:
        log.warning(f'Wikimedia failed for {name}: {e}')
    return photos

# ─── Venue website scraper + photo lookup (weekly) ───────────────────────────

def load_venue_status():
    if VENUE_STATUS_JSON.exists():
        try:
            with open(VENUE_STATUS_JSON, encoding='utf-8') as f:
                return json.load(f)
        except: pass
    return {}

def save_venue_status(status):
    with open(VENUE_STATUS_JSON, 'w', encoding='utf-8') as f:
        json.dump(status, f, ensure_ascii=False, indent=2)

def load_venues_from_csv():
    csv_path = DATA_DIR / 'latest_cafes.csv'
    if not csv_path.exists(): return []
    venues = []
    try:
        with open(csv_path, encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                website = row.get('website','').strip()
                if not website: continue
                venues.append({
                    'osm_id':  row.get('osm_id', row.get('name','')),
                    'name':    row.get('name',''),
                    'city':    row.get('city',''),
                    'website': website,
                    'lat':     row.get('lat',''),
                    'lon':     row.get('lon',''),
                })
    except Exception as e:
        log.warning(f'Could not load venues CSV: {e}')
    return venues

def run_venue_scrape():
    """Weekly: scrape venue websites and fetch photos for a batch."""
    log.info('── Weekly venue scrape ──')
    venues = load_venues_from_csv()
    status = load_venue_status()

    # Sort by least recently checked first
    def last_checked_key(v):
        s = status.get(v['osm_id'], {})
        return s.get('last_checked', '1970-01-01')

    venues.sort(key=last_checked_key)
    batch  = venues[:VENUES_PER_RUN]
    done   = 0

    for v in batch:
        vid     = v['osm_id'] or hashlib.md5(v['name'].encode()).hexdigest()[:10]
        website = v['website']
        name    = v['name']
        city    = v['city']
        lat     = v.get('lat','')
        lon     = v.get('lon','')

        # Ensure URL has protocol
        url = website if website.startswith('http') else f'https://{website}'
        out_dir = VENUE_DIR / vid

        log.info(f'  Venue: {name[:40]} — {url[:50]}')
        result = scrape_page(url, out_dir)

        # Fetch photos if we have coords
        photos = []
        if lat and lon:
            photos.extend(fetch_mapillary_photos(lat, lon, name))
            time.sleep(0.5)
            if len(photos) < 3:
                photos.extend(fetch_wikimedia_photos(name, city))

        old = status.get(vid, {})
        status[vid] = {
            'id':           vid,
            'name':         name,
            'website':      website,
            'last_checked': now_iso(),
            'last_changed': now_iso() if result.get('changed') else old.get('last_changed',''),
            'changed':      result.get('changed', False),
            'size_bytes':   result.get('size_bytes', 0),
            'error':        result.get('error'),
            'file':         result.get('file'),
            'txt':          result.get('txt'),
            'photos':       photos,
            'photo_count':  len(photos),
        }
        done += 1
        save_venue_status(status)  # save incrementally
        time.sleep(2)

    log.info(f'Venue scrape done: {done} venues processed, {len(status)} total in status')

# ─── Venue name loader ────────────────────────────────────────────────────────

def load_venue_names():
    csv_path = DATA_DIR / 'latest_cafes.csv'
    if not csv_path.exists(): return []
    names = []
    try:
        with open(csv_path, encoding='utf-8') as f:
            for line in f.readlines()[1:]:
                name = line.split(',')[0].strip().strip('"')
                if name and len(name) > 3: names.append(name)
    except Exception as e:
        log.warning(f'Could not load venues: {e}')
    return names

# ─── Main ─────────────────────────────────────────────────────────────────────

def main():
    now      = datetime.now()
    is_monthly = (now.day == 1)
    is_weekly  = (now.day % 7 == 1)  # days 1, 8, 15, 22, 29

    log.info('─' * 55)
    log.info(f'Updish fetch — {now.strftime("%Y-%m-%d %H:%M")}  '
             f'{"[MONTHLY] " if is_monthly else ""}'
             f'{"[WEEKLY] " if is_weekly else ""}')

    # ── Daily: critic feed ──────────────────────────────────────────────────
    existing, seen_ids = load_existing_feed()
    log.info(f'Existing feed: {len(existing)} entries')
    new_entries, all_outlinks = [], []

    for source in RSS_SOURCES:
        e, l = fetch_rss(source, seen_ids); new_entries.extend(e); all_outlinks.extend(l); time.sleep(1)
    for query in GOOGLE_NEWS_QUERIES:
        e, l = fetch_google_news(query, seen_ids); new_entries.extend(e); all_outlinks.extend(l)

    venue_names = load_venue_names()
    if venue_names:
        e, l = fetch_venue_news(venue_names, seen_ids); new_entries.extend(e); all_outlinks.extend(l)

    for sub in REDDIT_SUBS:
        e, l = fetch_reddit(sub, seen_ids); new_entries.extend(e); all_outlinks.extend(l)

    # ── Weekly: scrape new critic articles ──────────────────────────────────
    if is_weekly:
        all_entries_combined = new_entries + existing
        all_entries_combined = scrape_critic_articles(all_entries_combined)
        # separate back out
        existing_ids = set(e['id'] for e in existing)
        new_entries  = [e for e in all_entries_combined if e['id'] not in existing_ids]
        existing     = [e for e in all_entries_combined if e['id'] in existing_ids]

    all_entries = new_entries + existing
    all_entries.sort(key=lambda e: e.get('date',''), reverse=True)
    save_feed(all_entries[:MAX_ENTRIES])
    log.info(f'Feed: {len(new_entries)} new entries')

    # ── Daily: candidate discovery ──────────────────────────────────────────
    if all_outlinks:
        update_candidates(all_outlinks)

    # ── Weekly: venue scrape + photos ───────────────────────────────────────
    if is_weekly:
        run_venue_scrape()

    # ── Monthly: library check ──────────────────────────────────────────────
    if is_monthly:
        run_library_check()

    log.info('Done')
    log.info('─' * 55)

if __name__ == '__main__':
    main()
