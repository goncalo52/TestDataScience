import argparse
import gzip
import hashlib
import json
import logging
import os
import random
import time
from collections import Counter
from datetime import datetime, UTC
from urllib.parse import urlparse

import requests
from boilerpy3 import extractors

try:
    from deep_translator import GoogleTranslator
    translator = GoogleTranslator(source='auto', target='en')
except:
    translator = None

# Configuration
OUTPUT_DIR = "ukraine_war_data"
TARGET_ARTICLES = 100
MAX_REQUESTS = 1000
REQUEST_DELAY = 1.5

CRAWL_INDICES = [
    "CC-MAIN-2022-21",  # May 2022 (post-invasion)
    "CC-MAIN-2022-27",  # June/July 2022
    "CC-MAIN-2022-33",  # August 2022
    "CC-MAIN-2022-40",  # September/October 2022
]

DOMAINS = {
    "PT": ["publico.pt", "dn.pt"],
    "UA": ["ukrinform.net", "kyivpost.com"],
    "RU": ["ria.ru", "tass.ru"],
}


HIGH_KEYWORDS = [
    # English
    "ukraine invasion", "russia invades ukraine", "putin invades",
    "russian invasion", "ukraine war", "russia ukraine war",
    "ukrainian war", "kyiv attack", "kiev attack", "mariupol",
    "bucha", "irpin", "kharkiv", "zaporizhzhia", "odesa",
    "missile strike", "drone attack", "war crimes", "atrocities",
    "kherson counteroffensive", "bakhmut",
    
    # Portuguese (PT)
    "invasão russa", "guerra na ucrânia", "crimes de guerra",
    "massacre de bucha", "tropas russas", "ataque de mísseis",
    "contraofensiva",
    
    # Russian (RU)
    "донбас", "донбасс", "спецоперация", "война на украине",
    "ракетный обстрел", "территориальная целостность",
    
    # Ukrainian (UA)
    "російське вторгнення", "війна в україні", "ракетний удар",
    "збройні сили", "деокупація",
]

MEDIUM_KEYWORDS = [
    # English
    "ukraine", "russia", "putin", "zelensky", "zelenskyy",
    "russian troops", "ukrainian forces", "nato ukraine", "weapon supply",
    "donbas", "donbass", "kyiv", "kiev", "crimea", "sanctions",
    "oil embargo", "russian economy", "refugee crisis", "grain export",
    "mobilization", "international criminal court", "united nations",
    
    # Portuguese (PT)
    "sanções", "refugiados", "conflito", "ajuda militar",
    "economia russa", "crise de energia", "otan",
    
    # Russian (RU)
    "вооруженные силы", "санкции", "беженцы", "конфликт",
    "помощь украине", "кремль",
    
    # Ukrainian (UA)
    "нато", "путін", "допомога", "санкції", "зброя", "кремль",
    "президент зеленський",
]

EXCLUDE = [
    "recipe", "weather forecast", "sports", "football", "soccer",
    "celebrity", "fashion", "entertainment", "movie", "music",
    "poker", "gaming", "bitcoin", "cryptocurrency", "real estate market",
]

# Logging setup
def setup_logging(output_dir):
    log_dir = os.path.join(output_dir, "logs")
    os.makedirs(log_dir, exist_ok=True)
    
    timestamp = datetime.now(UTC).strftime('%Y%m%d_%H%M%S')
    log_file = os.path.join(log_dir, f"scrape_{timestamp}.log")
    
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s [%(levelname)s] %(message)s',
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler()
        ]
    )
    return logging.getLogger(__name__)

logger = logging.getLogger(__name__)

# HTTP Session
session = requests.Session()
session.headers.update({
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/120.0.0.0',
})

# Rate limiter
class RateLimiter:
    def __init__(self):
        self.count = 0
        self.last_request = 0
        self.blocked = False
    
    def wait(self):
        elapsed = time.time() - self.last_request
        if elapsed < REQUEST_DELAY:
            time.sleep(REQUEST_DELAY - elapsed)
    
    def record(self, status_code):
        self.count += 1
        self.last_request = time.time()
        if status_code in (403, 429):
            self.blocked = True
            logger.error(f"Blocked with status {status_code}")
    
    def can_continue(self):
        return not self.blocked and self.count < MAX_REQUESTS

limiter = RateLimiter()

# Stats
class Stats:
    def __init__(self):
        self.saved = 0
        self.processed = 0
        self.duplicates = 0
        self.errors = Counter()
        self.sources = Counter()
    
    def log_summary(self):
        logger.info("=" * 60)
        logger.info("SCRAPING SUMMARY")
        logger.info("=" * 60)
        logger.info(f"Articles saved: {self.saved}")
        logger.info(f"Records processed: {self.processed}")
        logger.info(f"Duplicates skipped: {self.duplicates}")
        logger.info(f"Requests made: {limiter.count}/{MAX_REQUESTS}")
        logger.info(f"Low relevance: {self.processed - self.saved - self.errors['extraction'] - self.duplicates}")
        
        if self.sources:
            logger.info("\nTop sources:")
            for source, count in self.sources.most_common(5):
                logger.info(f"  {source}: {count}")
        
        if self.errors:
            logger.info("\nErrors encountered:")
            for error, count in self.errors.most_common(5):
                logger.info(f"  {error}: {count}")
        
        if limiter.blocked:
            logger.warning("⚠ Run stopped early due to rate limiting")

stats = Stats()

# Progress tracking
def load_progress(output_dir):
    progress_file = os.path.join(output_dir, "progress.json")
    if os.path.exists(progress_file):
        try:
            with open(progress_file) as f:
                return json.load(f)
        except:
            pass
    return {"last_country": None, "last_domain": None, "total_saved": 0}

def save_progress(output_dir, country, domain, total_saved):
    progress_file = os.path.join(output_dir, "progress.json")
    os.makedirs(output_dir, exist_ok=True)
    with open(progress_file, 'w') as f:
        json.dump({
            "last_country": country,
            "last_domain": domain,
            "total_saved": total_saved,
            "last_run": datetime.now(UTC).isoformat()
        }, f, indent=2)

# Utilities
def md5_hash(text):
    return hashlib.md5(text.encode()).hexdigest()

def load_seen(path):
    if os.path.exists(path):
        try:
            with open(path) as f:
                return set(json.load(f))
        except:
            pass
    return set()

def save_seen(path, seen_set):
    os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
    with open(path, 'w') as f:
        json.dump(sorted(list(seen_set)), f, indent=2)

# CDX Query
def query_cdx(index, domain, limit=200):
    if not limiter.can_continue():
        return []
    
    url = f"https://index.commoncrawl.org/{index}-index"
    params = {"url": f"{domain}/*", "output": "json", "filter": "status:200", "limit": limit}
    
    limiter.wait()
    try:
        resp = session.get(url, params=params, timeout=45)
        limiter.record(resp.status_code)
        
        if resp.status_code == 200:
            return [json.loads(line) for line in resp.text.strip().split('\n') if line]
    except Exception as e:
        logger.debug(f"CDX query failed: {e}")
    
    return []

# Fetch WARC
def fetch_warc(warc_path, offset, length):
    if not limiter.can_continue():
        return None
    
    url = f"https://data.commoncrawl.org/{warc_path}"
    headers = {"Range": f"bytes={offset}-{int(offset) + int(length) - 1}"}
    
    limiter.wait()
    try:
        resp = session.get(url, headers=headers, timeout=60)
        limiter.record(resp.status_code)
        
        if resp.status_code in (200, 206):
            try:
                return gzip.decompress(resp.content).decode('utf-8', errors='ignore')
            except:
                return resp.content.decode('utf-8', errors='ignore')
    except Exception as e:
        logger.debug(f"WARC fetch failed: {e}")
    
    return None

# Extract article
def extract_article(html):
    try:
        extractor = extractors.ArticleExtractor()
        doc = extractor.get_doc(html)
        if doc.title and doc.content:
            return {"title": doc.title, "text": doc.content}
    except Exception as e:
        logger.debug(f"Extraction error: {e}")
    return None

# Relevance
def score_relevance(text, title=""):
    combined = f"{title} {text}".lower()
    score = sum(3 for kw in HIGH_KEYWORDS if kw in combined)
    score += sum(2 for kw in MEDIUM_KEYWORDS if kw in combined)
    score -= sum(5 for kw in EXCLUDE if kw in combined)
    
    matched = [kw for kw in HIGH_KEYWORDS + MEDIUM_KEYWORDS if kw in combined]
    return score, matched[:5]

# Translation
def translate(text, max_chars=4800):
    if not translator or not text:
        return None
    
    try:
        if len(text) > max_chars:
            text = text[:max_chars] + "..."
        result = translator.translate(text)
        time.sleep(random.uniform(0.1, 0.5))
        return result
    except Exception as e:
        logger.debug(f"Translation failed: {e}")
        stats.errors["translation"] += 1
        return None

# Process record
def process_record(record, country, seen):
    url = record.get("url", "")
    if not url.startswith("http"):
        return None
    
    url_hash = md5_hash(url)
    if url_hash in seen:
        stats.duplicates += 1
        return None
    
    stats.processed += 1
    time.sleep(random.uniform(0.5, 1.5))
    
    # Fetch WARC
    warc_data = fetch_warc(record.get("filename"), record.get("offset"), record.get("length"))
    if not warc_data:
        stats.errors["warc_fetch"] += 1
        return None
    
    # Extract HTML
    html_start = None
    for marker in ["<!doctype", "<html"]:
        idx = warc_data.lower().find(marker)
        if idx != -1:
            html_start = idx
            break
    html = warc_data[html_start:] if html_start else warc_data
    
    # Extract article
    article = extract_article(html)
    if not article or len(article.get("text", "")) < 100:
        stats.errors["extraction"] += 1
        return None
    
    title = article["title"]
    text = article["text"]
    
    logger.debug(f"Extracted: {title[:80]}... (length: {len(text)})")
    
    # Check relevance
    score, keywords = score_relevance(text, title)
    if score < 3:  # Lowered threshold to see more results
        logger.debug(f"Low relevance (score={score}): {title[:60]}")
        return None
    
    # Translate if needed
    title_en = title if country == "EN" else translate(title)
    text_en = text if country == "EN" else translate(text)
    
    # Clean text
    def clean(s):
        if not s:
            return None
        return ' '.join(s.replace('\n', ' ').replace('\t', ' ').split())
    
    result = {
        "url": url,
        "source": urlparse(url).netloc,
        "country": country,
        "title": clean(title),
        "title_en": clean(title_en),
        "text": clean(text),
        "text_en": clean(text_en),
        "score": score,
        "keywords": keywords,
        "timestamp": record.get("timestamp"),
        "scraped_at": datetime.now(UTC).isoformat(),
        "hash": url_hash
    }
    
    # Save
    day = datetime.now(UTC).strftime("%Y-%m-%d")
    out_path = os.path.join(OUTPUT_DIR, country, f"{day}.jsonl")
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    
    with open(out_path, 'a', encoding='utf-8') as f:
        json.dump(result, f, ensure_ascii=False)
        f.write('\n')
    
    seen.add(url_hash)
    stats.saved += 1
    stats.sources[result['source']] += 1
    
    logger.info(f"✓ Saved [score={score}]: {title[:60]}")
    return result

# Main scraper
def scrape(output_dir=OUTPUT_DIR, target=TARGET_ARTICLES):
    logger.info("=" * 60)
    logger.info("UKRAINE WAR NEWS SCRAPER")
    logger.info("=" * 60)
    logger.info(f"Target: {target} articles")
    logger.info(f"Max requests: {MAX_REQUESTS}")
    logger.info("=" * 60)
    
    # Load progress
    progress = load_progress(output_dir)
    start_country = progress.get("last_country")
    started = start_country is None
    
    if start_country:
        logger.info(f"Resuming from: {start_country}")
    
    for country, domains in DOMAINS.items():
        if stats.saved >= target or not limiter.can_continue():
            break
        
        # Check if we should start processing
        if not started:
            if country == start_country:
                started = True
            else:
                continue
        
        logger.info(f"\n>>> Processing country: {country}")
        
        seen_path = os.path.join(output_dir, country, "seen.json")
        seen = load_seen(seen_path)
        
        for domain in domains:
            if stats.saved >= target or not limiter.can_continue():
                break
            
            logger.info(f"  Domain: {domain}")
            
            for index in CRAWL_INDICES:
                if stats.saved >= target or not limiter.can_continue():
                    break
                
                records = query_cdx(index, domain)
                if not records:
                    continue
                
                logger.info(f"    Found {len(records)} records in {index}")
                
                for record in records[:50]:
                    if stats.saved >= target or not limiter.can_continue():
                        break
                    
                    process_record(record, country, seen)
                
                # Save progress after each index
                save_seen(seen_path, seen)
                save_progress(output_dir, country, domain, stats.saved)
    
    stats.log_summary()

# CLI
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Ukraine war news scraper")
    parser.add_argument("--out", default=OUTPUT_DIR, help="Output directory")
    parser.add_argument("--target", type=int, default=TARGET_ARTICLES, help="Target articles")
    parser.add_argument("--max-requests", type=int, default=MAX_REQUESTS, help="Max requests")
    args = parser.parse_args()
    
    MAX_REQUESTS = args.max_requests
    OUTPUT_DIR = args.out
    
    logger = setup_logging(OUTPUT_DIR)
    
    scrape(output_dir=OUTPUT_DIR, target=args.target)
