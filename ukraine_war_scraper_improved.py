#!/usr/bin/env python3
"""
ukraine_war_scraper_improved.py

Rate-limited scraper with better error handling and resumption support.
Designed to work with GitHub Actions for incremental collection.
"""

import argparse
import gzip
import hashlib
import json
import logging
import os
import random
import sys
import time
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from urllib.parse import urlparse
from boilerpy3 import extractors
import requests
from bs4 import BeautifulSoup

# Optional imports
try:
    from newspaper import Article
except:
    Article = None

try:
    from deep_translator import GoogleTranslator
except:
    GoogleTranslator = None

try:
    from langdetect import detect
except:
    detect = None

# ======================
# Configuration
# ======================
DEFAULT_OUTPUT = "ukraine_war_data"
DEFAULT_TARGET = 50  # Reduced for hourly runs
DEFAULT_WORKERS = 3  # Reduced to avoid rate limits
DEFAULT_REQUEST_DELAY = 2.0  # Increased delay
DEFAULT_CDX_LIMIT = 100  # Reduced per query

# Rate limiting
MAX_REQUESTS_PER_RUN = 200  # Hard limit per execution
MIN_REQUEST_INTERVAL = 1.5  # Minimum seconds between requests
MAX_RETRIES = 3
BACKOFF_FACTOR = 3

# Common Crawl indices
COMMONCRAWL_INDICES = [
    "CC-MAIN-2022-05",
    "CC-MAIN-2022-21",
]

TARGET_DOMAINS = {
    "PT": ["publico.pt", "dn.pt"],
    "UA": ["ukrinform.net", "kyivpost.com"],
    "RU": ["ria.ru", "tass.ru"],
    "EN": ["bbc.com", "reuters.com"]
}

HIGH_RELEVANCE_KEYWORDS = [
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

MEDIUM_RELEVANCE_KEYWORDS = [
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

EXCLUDE_KEYWORDS = [
    # Keep these standard exclusions
    "recipe", "weather forecast", "sports", "football", "soccer",
    "celebrity", "fashion", "entertainment", "movie", "music",
    "poker", "gaming", "bitcoin", "cryptocurrency", "real estate market",
]
# ======================
# Logging
# ======================
def setup_logging(out_dir):
    """Setup logging to both console and file."""
    # Create logs directory
    log_dir = os.path.join(out_dir, "logs")
    ensure_dir(log_dir)
    
    # Log filename with timestamp
    log_file = os.path.join(
        log_dir, 
        f"scraper_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.log"
    )
    
    # Create formatters
    detailed_formatter = logging.Formatter(
        '%(asctime)s [%(levelname)-8s] %(funcName)s:%(lineno)d - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    simple_formatter = logging.Formatter(
        '%(asctime)s %(levelname)-8s %(message)s',
        datefmt='%H:%M:%S'
    )
    
    # File handler (detailed, includes DEBUG)
    file_handler = logging.FileHandler(log_file, encoding='utf-8')
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(detailed_formatter)
    
    # Console handler (simple, INFO and above)
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(simple_formatter)
    
    # Setup logger
    logger = logging.getLogger("ukraine_scraper")
    logger.setLevel(logging.DEBUG)
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)
    
    logger.info(f"Logging to: {log_file}")
    return logger

# Initialize with default logger (will be reconfigured in main)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger("ukraine_scraper")

# ======================
# Request tracker with rate limiting
# ======================
class RequestTracker:
    def __init__(self, max_requests=MAX_REQUESTS_PER_RUN):
        self.request_count = 0
        self.max_requests = max_requests
        self.last_request_time = 0
        self.blocked = False
        
    def can_make_request(self):
        if self.blocked:
            return False
        if self.request_count >= self.max_requests:
            logger.warning(f"Hit max requests limit ({self.max_requests})")
            return False
        return True
    
    def wait_if_needed(self):
        """Enforce minimum interval between requests."""
        now = time.time()
        elapsed = now - self.last_request_time
        if elapsed < MIN_REQUEST_INTERVAL:
            sleep_time = MIN_REQUEST_INTERVAL - elapsed
            time.sleep(sleep_time)
    
    def record_request(self, status_code=None):
        self.request_count += 1
        self.last_request_time = time.time()
        
        # Check for rate limiting/blocking
        if status_code == 429:
            logger.error("Rate limited (429) - stopping execution")
            self.blocked = True
        elif status_code == 403:
            logger.error("Forbidden (403) - possible IP block")
            self.blocked = True
    
    def is_blocked(self):
        return self.blocked

# Global tracker
tracker = RequestTracker()

# ======================
# Statistics
# ======================
class ScraperStats:
    def __init__(self):
        self.cdx_queries = 0
        self.records_fetched = 0
        self.records_processed = 0
        self.articles_extracted = 0
        self.articles_relevant = 0
        self.articles_saved = 0
        self.duplicates_skipped = 0
        self.errors = Counter()
        self.sources = Counter()
        self.blocked = False
        
    def log_summary(self):
        logger.info("=" * 60)
        logger.info("SCRAPING SUMMARY")
        logger.info("=" * 60)
        logger.info(f"Requests made: {tracker.request_count}/{tracker.max_requests}")
        logger.info(f"CDX queries: {self.cdx_queries}")
        logger.info(f"Records fetched: {self.records_fetched}")
        logger.info(f"Records processed: {self.records_processed}")
        logger.info(f"Articles extracted: {self.articles_extracted}")
        logger.info(f"Articles relevant: {self.articles_relevant}")
        logger.info(f"Articles saved: {self.articles_saved}")
        logger.info(f"Duplicates skipped: {self.duplicates_skipped}")
        if self.blocked or tracker.is_blocked():
            logger.info("⚠ BLOCKED/RATE LIMITED - Run stopped early")
        if self.sources:
            logger.info("\nTop sources:")
            for source, count in self.sources.most_common(5):
                logger.info(f"  {source}: {count}")
        if self.errors:
            logger.info("\nErrors:")
            for error, count in self.errors.most_common(5):
                logger.info(f"  {error}: {count}")

# ======================
# Utilities
# ======================
def ensure_dir(path):
    os.makedirs(path, exist_ok=True)

def md5(text: str):
    return hashlib.md5(text.encode("utf-8")).hexdigest()

def now_iso():
    return datetime.utcnow().isoformat() + "Z"

# ======================
# Session with User-Agent
# ======================
def create_session():
    """Create requests session with browser-like headers."""
    session = requests.Session()
    session.headers.update({
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Accept': 'application/json, text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.9',
        'Accept-Encoding': 'gzip, deflate',
        'Connection': 'keep-alive',
    })
    return session

session = create_session()

# ======================
# Progress tracking
# ======================
def load_progress(out_dir):
    """Load progress state to resume from."""
    progress_file = os.path.join(out_dir, "progress.json")
    if os.path.exists(progress_file):
        try:
            with open(progress_file, "r") as f:
                return json.load(f)
        except:
            pass
    return {
        "last_country": None,
        "last_domain": None,
        "last_index": None,
        "total_saved": 0
    }

def save_progress(out_dir, country, domain, index, total_saved):
    """Save progress state."""
    progress_file = os.path.join(out_dir, "progress.json")
    ensure_dir(out_dir)
    with open(progress_file, "w") as f:
        json.dump({
            "last_country": country,
            "last_domain": domain,
            "last_index": index,
            "total_saved": total_saved,
            "last_run": now_iso()
        }, f, indent=2)

# ======================
# Seen articles
# ======================
def load_seen(path):
    if os.path.exists(path):
        try:
            with open(path, "r", encoding="utf-8") as f:
                return set(json.load(f))
        except:
            return set()
    return set()

def save_seen(path, seen_set):
    ensure_dir(os.path.dirname(path) or ".")
    with open(path, "w", encoding="utf-8") as f:
        json.dump(sorted(list(seen_set)), f, indent=2)

# ======================
# CDX querying with improved error handling
# ======================
def query_cdx(index, domain, max_results=DEFAULT_CDX_LIMIT):
    """Query CDX with rate limiting and error handling."""
    if not tracker.can_make_request():
        return []
    
    endpoints = [
        f"https://index.commoncrawl.org/{index}-index",
        f"https://index.commoncrawl.org/{index}",
    ]
    
    params = {
        "url": f"{domain}/*",
        "output": "json",
        "filter": "status:200",
        "limit": max_results,
    }
    
    for attempt in range(MAX_RETRIES):
        for url in endpoints:
            try:
                tracker.wait_if_needed()
                
                resp = session.get(url, params=params, timeout=45)
                tracker.record_request(resp.status_code)
                
                if resp.status_code == 200:
                    lines = [line for line in resp.text.splitlines() if line.strip()]
                    if not lines:
                        continue
                    
                    results = []
                    for l in lines:
                        try:
                            results.append(json.loads(l))
                        except:
                            continue
                    
                    if results:
                        return results
                        
                elif resp.status_code in (429, 403):
                    # Stop immediately if blocked
                    return []
                    
            except requests.exceptions.Timeout:
                logger.debug(f"CDX timeout (attempt {attempt+1}/{MAX_RETRIES})")
                if attempt < MAX_RETRIES - 1:
                    time.sleep(BACKOFF_FACTOR ** attempt)
            except Exception as e:
                logger.debug(f"CDX error: {e}")
                if attempt < MAX_RETRIES - 1:
                    time.sleep(BACKOFF_FACTOR ** attempt)
    
    return []

# ======================
# WARC fetching
# ======================
def fetch_warc_range(warc_path, offset, length):
    """Fetch byte range with rate limiting."""
    if not tracker.can_make_request():
        return None
    
    url = f"https://data.commoncrawl.org/{warc_path}"
    headers = {"Range": f"bytes={offset}-{int(offset) + int(length) - 1}"}
    
    for attempt in range(MAX_RETRIES):
        try:
            tracker.wait_if_needed()
            
            r = session.get(url, headers=headers, timeout=60)
            tracker.record_request(r.status_code)
            
            if r.status_code in (200, 206):
                try:
                    content = gzip.decompress(r.content)
                    return content.decode("utf-8", errors="ignore")
                except:
                    try:
                        return r.content.decode("utf-8", errors="ignore")
                    except:
                        return None
            elif r.status_code in (429, 403):
                return None
                
        except Exception as e:
            if attempt < MAX_RETRIES - 1:
                time.sleep(BACKOFF_FACTOR ** attempt)
            else:
                logger.debug(f"WARC fetch failed: {e}")
    
    return None

# ======================
# Article extraction (simplified)
# ======================
def extract_article(html, url):
    try:
        extractor = extractors.ArticleExtractor()
        doc = extractor.get_doc(html)
        
        return {
            "title": doc.title,
            "text": doc.content
        }
    except Exception as e:
        logger.debug(f"boilerpy3 extraction failed for {url}: {e}")
        return None

# ======================
# Relevance scoring
# ======================
def calculate_relevance_score(text, title=""):
    combined = f"{title} {text}".lower()
    score = 0
    matched = []
    
    for kw in HIGH_RELEVANCE_KEYWORDS:
        if kw in combined:
            score += 3
            matched.append(kw)
    
    for kw in MEDIUM_RELEVANCE_KEYWORDS:
        if kw in combined:
            score += 2
            matched.append(kw)
    
    for kw in EXCLUDE_KEYWORDS:
        if kw in combined:
            score -= 5
    
    return score, matched

def is_relevant(text, title="", min_score=5):
    score, _ = calculate_relevance_score(text, title)
    return score >= min_score

# ======================
# Process record
# ======================
def process_record(record, country, seen_set, out_dir, stats):
    """Process a single CDX record."""
    if tracker.is_blocked():
        return None
    
    url = record.get("url")
    if not url or not url.startswith("http"):
        return None
    
    url_hash = md5(url)
    if url_hash in seen_set:
        stats.duplicates_skipped += 1
        return None
    
    warc = record.get("filename")
    offset = record.get("offset")
    length = record.get("length")
    if not warc or offset is None or length is None:
        return None
    
    stats.records_processed += 1
    
    # Add small random delay
    time.sleep(random.uniform(0.5, 1.5))
    
    warc_content = fetch_warc_range(warc, offset, length)
    if not warc_content:
        stats.errors["warc_fetch_failed"] += 1
        return None
    
    # Extract HTML
    idx = None
    for marker in ["<!doctype", "<html", "<HTML"]:
        i = warc_content.lower().find(marker.lower())
        if i != -1:
            idx = i
            break
    html = warc_content[idx:] if idx is not None else warc_content
    
    article = extract_article(html, url)
    if not article or not article.get("text") or len(article.get("text", "")) < 100:
        stats.errors["extraction_failed"] += 1
        return None
    
    stats.articles_extracted += 1
    
    title = article.get("title", "")
    text = article.get("text", "")
    
    if not is_relevant(text, title):
        return None
    
    stats.articles_relevant += 1
    
    score, keywords = calculate_relevance_score(text, title)
    
    result = {
        "url": url,
        "source": urlparse(url).netloc,
        "country": country,
        "title": title,
        "text": text,
        "crawl_timestamp": record.get("timestamp"),
        "relevance_score": score,
        "matched_keywords": keywords[:5],
        "scraped_at": now_iso(),
        "hash": url_hash,
    }
    
    # Save
    try:
            # Save as one line of JSON (JSON Lines format) for appending
            day = datetime.utcnow().strftime("%Y-%m-%d")
            country_dir = os.path.join(out_dir, country)
            ensure_dir(country_dir)
            # Use .jsonl extension for clarity (or keep .json if you prefer)
            file_path = os.path.join(country_dir, f"{day}.jsonl") 
            
            # Use 'a' for append mode, '\n' to ensure separate lines
            # and flush=True for immediate write
            with open(file_path, "a", encoding="utf-8") as f: 
                # Dumps the article result as a single line JSON object
                json.dump(result, f, ensure_ascii=False) 
                f.write('\n') # Newline separator
                f.flush()
            
            seen_set.add(url_hash)
            stats.articles_saved += 1
            stats.sources[result['source']] += 1
            
            logger.info(f"✓ Saved (score={score}): {title[:60]}...")
            
            return result
            
    except Exception as e:
        stats.errors["save_failed"] += 1
        logger.error(f"Error saving article: {e}") # Added specific error logging
        return None

# ======================
# Main scraper
# ======================
def run_scraper(domains_map,
                out_dir=DEFAULT_OUTPUT,
                target_limit=DEFAULT_TARGET,
                workers=DEFAULT_WORKERS,
                indices=None,
                cdx_limit=DEFAULT_CDX_LIMIT):
    
    indices = indices or COMMONCRAWL_INDICES
    ensure_dir(out_dir)
    stats = ScraperStats()
    
    # Load progress
    progress = load_progress(out_dir)
    start_country = progress.get("last_country")
    start_domain = progress.get("last_domain")
    
    logger.info("=" * 60)
    logger.info("UKRAINE WAR NEWS SCRAPER (Rate-Limited)")
    logger.info("=" * 60)
    logger.info(f"Target: {target_limit} articles")
    logger.info(f"Max requests: {tracker.max_requests}")
    logger.info(f"Workers: {workers}")
    if start_country:
        logger.info(f"Resuming from: {start_country}/{start_domain}")
    logger.info("=" * 60)
    
    started = start_country is None
    
    for country, domains in domains_map.items():
        if stats.articles_saved >= target_limit or tracker.is_blocked():
            break
        
        if not started:
            if country == start_country:
                started = True
            else:
                continue
        
        logger.info(f"\n>>> Processing {country}...")
        
        country_seen_path = os.path.join(out_dir, country, "seen_articles.json")
        seen_set = load_seen(country_seen_path)
        
        for domain in domains:
            if stats.articles_saved >= target_limit or tracker.is_blocked():
                break
            
            logger.info(f"\n  Domain: {domain}")
            
            for idx in indices:
                if stats.articles_saved >= target_limit or tracker.is_blocked():
                    break
                
                stats.cdx_queries += 1
                records = query_cdx(idx, domain, max_results=cdx_limit)
                
                if not records:
                    continue
                
                logger.info(f"    Found {len(records)} records in {idx}")
                stats.records_fetched += len(records)
                
                # Process records sequentially (safer for rate limiting)
                for rec in records[:50]:
                    if stats.articles_saved >= target_limit or tracker.is_blocked():
                        break
                    
                    process_record(rec, country, seen_set, out_dir, stats)
                
                save_seen(country_seen_path, seen_set)
                save_progress(out_dir, country, domain, idx, stats.articles_saved)
    
    logger.info("\n")
    stats.log_summary()

# ======================
# CLI
# ======================
def main():
    p = argparse.ArgumentParser(description="Rate-limited Ukraine war scraper")
    p.add_argument("--out", default=DEFAULT_OUTPUT, help="Output directory")
    p.add_argument("--target", type=int, default=DEFAULT_TARGET, help="Target articles")
    p.add_argument("--workers", type=int, default=DEFAULT_WORKERS, help="Workers")
    p.add_argument("--max-requests", type=int, default=MAX_REQUESTS_PER_RUN, 
                   help="Max requests per run")
    args = p.parse_args()
    
    tracker.max_requests = args.max_requests
    
    run_scraper(
        domains_map=TARGET_DOMAINS,
        out_dir=args.out,
        target_limit=args.target,
        workers=args.workers,
    )

if __name__ == "__main__":
    main()
