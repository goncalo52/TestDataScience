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
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, UTC
from urllib.parse import urlparse
from boilerpy3 import extractors
import requests

# Optional imports
try:
    from newspaper import Article
except ImportError:
    Article = None

try:
    from deep_translator import GoogleTranslator
except ImportError:
    GoogleTranslator = None

try:
    from langdetect import detect
except ImportError:
    detect = None

# ======================
# Configuration Constants
# ======================
class Config:
    DEFAULT_OUTPUT = "ukraine_war_data"
    DEFAULT_TARGET = 50
    DEFAULT_WORKERS = 3
    DEFAULT_CDX_LIMIT = 200
    
    # Rate limiting
    MAX_REQUESTS_PER_RUN = 200
    MIN_REQUEST_INTERVAL = 1.5
    MAX_RETRIES = 3
    BACKOFF_FACTOR = 3

    COMMONCRAWL_INDICES = ["CC-MAIN-2022-05", "CC-MAIN-2022-21"]

    TARGET_DOMAINS = {
        "PT": ["publico.pt", "dn.pt"],
        "UA": ["ukrinform.net", "kyivpost.com"],
        "RU": ["ria.ru", "tass.ru"],
        "EN": ["bbc.com", "reuters.com"]
    }
    
    HIGH_RELEVANCE_KEYWORDS = [
        "ukraine invasion", "russia invades ukraine", "putin invades",
        "russian invasion", "ukraine war", "russia ukraine war",
        "ukrainian war", "kyiv attack", "kiev attack", "mariupol",
        "bucha", "irpin", "kharkiv", "zaporizhzhia", "odesa",
        "missile strike", "drone attack", "war crimes", "atrocities",
        "kherson counteroffensive", "bakhmut",
        "invasão russa", "guerra na ucrânia", "crimes de guerra",
        "massacre de bucha", "tropas russas", "ataque de mísseis",
        "contraofensiva",
        "донбас", "донбасс", "спецоперация", "война на украине",
        "ракетный обстрел", "территориальная целостность",
        "російське вторгнення", "війна в україні", "ракетний удар",
        "збройні сили", "деокупація",
    ]

    MEDIUM_RELEVANCE_KEYWORDS = [
        "ukraine", "russia", "putin", "zelensky", "zelenskyy",
        "russian troops", "ukrainian forces", "nato ukraine", "weapon supply",
        "donbas", "donbass", "kyiv", "kiev", "crimea", "sanctions",
        "oil embargo", "russian economy", "refugee crisis", "grain export",
        "mobilization", "international criminal court", "united nations",
        "sanções", "refugiados", "conflito", "ajuda militar",
        "economia russa", "crise de energia", "otan",
        "вооруженные силы", "санкции", "беженцы", "конфликт",
        "помощь украине", "кремль",
        "нато", "путін", "допомога", "санкції", "зброя", "кремль",
        "президент зеленський",
    ]

    EXCLUDE_KEYWORDS = [
        "recipe", "weather forecast", "sports", "football", "soccer",
        "celebrity", "fashion", "entertainment", "movie", "music",
        "poker", "gaming", "bitcoin", "cryptocurrency", "real estate market",
    ]


# ======================
# Utilities
# ======================
def ensure_dir(path):
    os.makedirs(path, exist_ok=True)

def md5(text: str):
    return hashlib.md5(text.encode("utf-8")).hexdigest()

def now_iso():
    return datetime.now(UTC).isoformat().replace('+00:00', 'Z')

def _rw_json(path, data=None):
    """Helper for reading/writing JSON files (progress, seen)."""
    ensure_dir(os.path.dirname(path) or ".")
    if data is None:
        if os.path.exists(path):
            try:
                with open(path, "r") as f:
                    return json.load(f)
            except:
                pass
        return {}
    else:
        with open(path, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2)

# ======================
# Logging Setup
# ======================
def setup_logging(out_dir):
    """Setup logging to both console and file."""
    log_dir = os.path.join(out_dir, "logs")
    ensure_dir(log_dir)
    
    log_file = os.path.join(
        log_dir, 
        f"scraper_{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}.log"
    )
    
    detailed_formatter = logging.Formatter(
        '%(asctime)s [%(levelname)-8s] %(funcName)s:%(lineno)d - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    logger = logging.getLogger("ukraine_scraper")
    logger.setLevel(logging.DEBUG)
    
    file_handler = logging.FileHandler(log_file, encoding='utf-8')
    file_handler.setLevel(logging.DEBUG)
    file_handler.setFormatter(detailed_formatter)
    logger.addHandler(file_handler)
    
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.INFO)
    console_handler.setFormatter(logging.Formatter(
        '%(asctime)s %(levelname)-8s %(message)s',
        datefmt='%H:%M:%S'
    ))
    logger.addHandler(console_handler)
    
    logger.info(f"Logging to: {log_file}")
    return logger

def init_translator(logger):
    translator = None
    if GoogleTranslator:
        logger.info("Deep-translator module FOUND.")
        try:
            translator = GoogleTranslator(source='auto', target='en')
            logger.info("Translation enabled using GoogleTranslator.")
        except Exception as e:
            logger.warning(f"Failed to initialize GoogleTranslator: {e}")
            translator = None
    return translator

# ======================
# Request tracker with rate limiting
# ======================
class RequestTracker:
    def __init__(self, max_requests=Config.MAX_REQUESTS_PER_RUN):
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
        if elapsed < Config.MIN_REQUEST_INTERVAL:
            time.sleep(Config.MIN_REQUEST_INTERVAL - elapsed)
    
    def record_request(self, status_code=None):
        self.request_count += 1
        self.last_request_time = time.time()
        
        if status_code in (429, 403):
            logger.error(f"Request blocked ({status_code}) - stopping execution")
            self.blocked = True
    
    def is_blocked(self):
        return self.blocked

# Global tracker (initialized in main)
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
        logger.info(f"Articles saved: {self.articles_saved} (Target: {Config.DEFAULT_TARGET})")
        logger.info(f"Duplicates skipped: {self.duplicates_skipped}")
        
        if tracker.is_blocked():
            logger.info("⚠ BLOCKED/RATE LIMITED - Run stopped early")
        
        if self.sources:
            logger.info("\nTop sources:")
            for source, count in self.sources.most_common(5):
                logger.info(f"  {source}: {count}")
        if self.errors:
            logger.info("\nErrors:")
            for error, count in self.errors.most_common(3):
                logger.info(f"  {error}: {count}")

# ======================
# Session
# ======================
def create_session():
    """Create requests session with browser-like headers."""
    session = requests.Session()
    session.headers['User-Agent'] = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
    return session

session = create_session()

# ======================
# Progress and Seen
# ======================
def load_progress(out_dir):
    """Load progress state to resume from."""
    return _rw_json(os.path.join(out_dir, "progress.json")) or {
        "last_country": None,
        "last_domain": None,
        "last_index": None,
        "total_saved": 0
    }

def save_progress(out_dir, country, domain, index, total_saved):
    """Save progress state."""
    _rw_json(os.path.join(out_dir, "progress.json"), {
        "last_country": country,
        "last_domain": domain,
        "last_index": index,
        "total_saved": total_saved,
        "last_run": now_iso()
    })

def load_seen(path):
    """Load seen article hashes."""
    data = _rw_json(path)
    return set(data) if isinstance(data, list) else set()

def save_seen(path, seen_set):
    """Save seen article hashes."""
    _rw_json(path, sorted(list(seen_set)))

# ======================
# CDX querying
# ======================
def query_cdx(index, domain, max_results=Config.DEFAULT_CDX_LIMIT):
    """Query CDX with rate limiting and error handling."""
    if not tracker.can_make_request():
        return []
    
    endpoints = [
        f"https://index.commoncrawl.org/{index}-index",
        f"https://index.commoncrawl.org/{index}",
    ]
    params = {
        "url": f"{domain}/*", "output": "json",
        "filter": "status:200", "limit": max_results,
    }
    
    for attempt in range(Config.MAX_RETRIES):
        for url in endpoints:
            try:
                tracker.wait_if_needed()
                resp = session.get(url, params=params, timeout=45)
                tracker.record_request(resp.status_code)
                
                if resp.status_code == 200:
                    lines = resp.text.splitlines()
                    results = [json.loads(l) for l in lines if l.strip()]
                    if results: return results
                        
                elif resp.status_code in (429, 403):
                    return [] # Stop immediately
                    
            except requests.exceptions.Timeout:
                logger.debug(f"CDX timeout (attempt {attempt+1})")
            except Exception as e:
                logger.debug(f"CDX error: {e}")
            
            if attempt < Config.MAX_RETRIES - 1:
                time.sleep(Config.BACKOFF_FACTOR ** attempt)
    
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
    
    for attempt in range(Config.MAX_RETRIES):
        try:
            tracker.wait_if_needed()
            r = session.get(url, headers=headers, timeout=60)
            tracker.record_request(r.status_code)
            
            if r.status_code in (200, 206):
                content = gzip.decompress(r.content)
                return content.decode("utf-8", errors="ignore")
            elif r.status_code in (429, 403):
                return None
                
        except Exception as e:
            if attempt < Config.MAX_RETRIES - 1:
                time.sleep(Config.BACKOFF_FACTOR ** attempt)
            else:
                logger.debug(f"WARC fetch failed: {e}")
    
    return None

# ======================
# Article extraction & scoring
# ======================
def extract_article(html, url):
    try:
        extractor = extractors.ArticleExtractor()
        doc = extractor.get_doc(html)
        return {"title": doc.title, "text": doc.content}
    except Exception as e:
        logger.debug(f"boilerpy3 extraction failed for {url}: {e}")
        return None

def calculate_relevance_score(text, title=""):
    combined = f"{title} {text}".lower()
    score = 0
    matched = []
    
    for kw in Config.HIGH_RELEVANCE_KEYWORDS:
        if kw in combined:
            score += 3
            matched.append(kw)
    
    for kw in Config.MEDIUM_RELEVANCE_KEYWORDS:
        if kw in combined:
            score += 2
            matched.append(kw)
    
    for kw in Config.EXCLUDE_KEYWORDS:
        if kw in combined:
            score -= 5
    
    return score, matched

def is_relevant(text, title="", min_score=4):
    score, _ = calculate_relevance_score(text, title)
    return score >= min_score

# ======================
# Translation
# ======================
def translate_text(text_to_translate, translator):
    """Translates text to English using GoogleTranslator, handling errors."""
    if not translator: return None, "translator_not_available"
    
    MAX_TRANSLATION_CHARS = 4800 
    
    try:
        if len(text_to_translate) > MAX_TRANSLATION_CHARS:
            text_to_translate = text_to_translate[:MAX_TRANSLATION_CHARS] + "..."
            
        translated_text = translator.translate(text_to_translate)
        time.sleep(random.uniform(0.1, 0.5))
        
        return translated_text, None
    except Exception as e:
        logger.warning(f"Translation failed: {type(e).__name__} - {e}")
        return None, type(e).__name__

# ======================
# Process record
# ======================
def process_record(record, country, seen_set, out_dir, stats, translator):
    """Process a single CDX record."""
    if tracker.is_blocked(): return None
    
    url = record.get("url")
    if not url or not url.startswith("http"): return None

    # Skip low-quality URLs immediately
    if url.strip('/').endswith(urlparse(url).netloc) or any(p in url for p in ['/block-lastnews', '/info/']):
        url_hash = md5(url) # Hash to skip it next time too
    else:
        url_hash = md5(url)
    
    if url_hash in seen_set:
        stats.duplicates_skipped += 1
        return None
    
    warc = record.get("filename")
    offset = record.get("offset")
    length = record.get("length")
    if not warc or offset is None or length is None: return None
    
    stats.records_processed += 1
    time.sleep(random.uniform(0.5, 1.5))
    
    warc_content = fetch_warc_range(warc, offset, length)
    if not warc_content:
        stats.errors["warc_fetch_failed"] += 1
        return None
    
    # Extract HTML
    idx = -1
    for marker in ["<!doctype", "<html", "<HTML"]:
        i = warc_content.lower().find(marker.lower())
        if i != -1:
            idx = i
            break
    html = warc_content[idx:] if idx != -1 else warc_content
    
    article = extract_article(html, url)
    if not article or len(article.get("text", "")) < 100:
        stats.errors["extraction_failed"] += 1
        return None
    
    stats.articles_extracted += 1
    
    title = article.get("title", "")
    text = article.get("text", "")
    
    if not is_relevant(text, title): return None
    
    stats.articles_relevant += 1
    score, keywords = calculate_relevance_score(text, title)
    
    # Translation Logic (Optimized for one API call)
    title_en, text_en, translation_status = None, None, "not_attempted"

    if country == "EN":
        title_en, text_en, translation_status = title, text, "native_english"
    elif translator:
        # Use a unique, unlikely marker to combine title and text for one API call
        MARKER = "__TITLE_AND_TEXT_SEP__"
        combined_text_to_translate = f"{title.strip()} {MARKER} {text.strip()}"
        
        translated_combined, translation_error = translate_text(combined_text_to_translate, translator)
            
        if translation_error:
            translation_status = "failed"
            stats.errors["translation_failed"] += 1
            title_en, text_en = None, None
        else:
            try:
                # Attempt to split the translated text using the marker
                parts = translated_combined.split(MARKER, 1)
                
                if len(parts) == 2:
                    title_en = parts[0].strip()
                    text_en = parts[1].strip()
                    translation_status = "success"
                else:
                    # Fallback if marker was translated/removed
                    logger.warning("Translation marker was not preserved. Storing combined text in both fields.")
                    title_en, text_en = translated_combined, translated_combined
                    translation_status = "partial_success"
                    stats.errors["translation_split_failed"] += 1
            except Exception as e:
                logger.error(f"Error splitting translated text: {e}")
                title_en, text_en = None, None
                translation_status = "split_error"
                stats.errors["translation_split_error"] += 1
    else:
        translation_status = "translator_not_available"
        
    def clean_text(s):
        """Clean and normalize text."""
        if s is None: return None
        s = s.replace('\n', '\\n').replace('\r', '').replace('\t', ' ')
        return ' '.join(s.split()).strip()

    result = {
        "url": url, "source": urlparse(url).netloc, "country": country,
        "title": clean_text(title), "title_en": clean_text(title_en), 
        "text": clean_text(text), "text_en": clean_text(text_en),   
        "translation_status": translation_status, 
        "crawl_timestamp": record.get("timestamp"),
        "relevance_score": score, "matched_keywords": keywords[:5],
        "scraped_at": now_iso(), "hash": url_hash,
    }
    
    # Save
    try:
        day = datetime.now(UTC).strftime("%Y-%m-%d")
        country_dir = os.path.join(out_dir, country)
        ensure_dir(country_dir)
        file_path = os.path.join(country_dir, f"{day}.jsonl") 
        
        with open(file_path, "a", encoding="utf-8") as f: 
            json.dump(result, f, ensure_ascii=False) 
            f.write('\n')
        
        seen_set.add(url_hash)
        stats.articles_saved += 1
        stats.sources[result['source']] += 1
        logger.info(f"✓ Saved (score={score}): {title[:60]}...")
        
        return result
            
    except Exception as e:
        stats.errors["save_failed"] += 1
        logger.error(f"Error saving article: {e}")
        return None
        
# ======================
# Main scraper
# ======================
def run_scraper(domains_map,
                out_dir=Config.DEFAULT_OUTPUT,
                target_limit=Config.DEFAULT_TARGET,
                workers=Config.DEFAULT_WORKERS,
                indices=None,
                cdx_limit=Config.DEFAULT_CDX_LIMIT):
    
    indices = indices or Config.COMMONCRAWL_INDICES
    ensure_dir(out_dir)
    stats = ScraperStats()
    global logger, translator

    logger = setup_logging(out_dir)
    translator = init_translator(logger)
    
    progress = load_progress(out_dir)
    start_country, start_domain = progress.get("last_country"), progress.get("last_domain")
    
    logger.info("=" * 60)
    logger.info("UKRAINE WAR NEWS SCRAPER (Rate-Limited)")
    logger.info("=" * 60)
    logger.info(f"Target: {target_limit} articles | Max requests: {tracker.max_requests} | Workers: {workers}")
    if start_country:
        logger.info(f"Resuming from: {start_country}/{start_domain}")
    logger.info("=" * 60)
    
    started = start_country is None
    
    for country, domains in domains_map.items():
        if stats.articles_saved >= target_limit or tracker.is_blocked(): break
        
        if not started:
            if country == start_country: started = True
            else: continue
        
        logger.info(f"\n>>> Processing {country}...")
        
        country_seen_path = os.path.join(out_dir, country, "seen_articles.json")
        seen_set = load_seen(country_seen_path)
        
        for domain in domains:
            if stats.articles_saved >= target_limit or tracker.is_blocked(): break
            
            logger.info(f"\n  Domain: {domain}")
            
            for idx in indices:
                if stats.articles_saved >= target_limit or tracker.is_blocked(): break
                
                stats.cdx_queries += 1
                records = query_cdx(idx, domain, max_results=cdx_limit)
                
                if not records: continue
                
                logger.info(f"    Found {len(records)} records in {idx}")
                stats.records_fetched += len(records)
                
                # --- START: Dynamic Record Limit Logic ---
                remaining_needed = target_limit - stats.articles_saved
                
                # Set a reasonable upper limit for processing. We assume a low success rate
                # (e.g., 1 out of 10 fetched records is a relevant, unique article).
                # We also cap it at 50, which was the previous hard limit.
                ASSUMED_SUCCESS_RATE_INVERSE = 10
                dynamic_limit = remaining_needed * ASSUMED_SUCCESS_RATE_INVERSE
                
                # Cap the dynamic limit to be no more than the CDX results fetched (200)
                # and the old hardcoded limit (50) for safety/efficiency.
                records_to_process = min(len(records), 50, max(5, dynamic_limit)) 
                
                logger.debug(f"    Processing {records_to_process} records (Needed: {remaining_needed})")
                # --- END: Dynamic Record Limit Logic ---
                
                # Process records sequentially
                for rec in records[:records_to_process]:
                    if stats.articles_saved >= target_limit or tracker.is_blocked(): break
                    
                    process_record(rec, country, seen_set, out_dir, stats, translator)
                
                save_seen(country_seen_path, seen_set)
                save_progress(out_dir, country, domain, idx, stats.articles_saved)
    
    logger.info("\n")
    stats.log_summary()
# ======================
# CLI
# ======================
def main():
    p = argparse.ArgumentParser(description="Rate-limited Ukraine war scraper")
    p.add_argument("--out", default=Config.DEFAULT_OUTPUT, help="Output directory")
    p.add_argument("--target", type=int, default=Config.DEFAULT_TARGET, help="Target articles")
    p.add_argument("--workers", type=int, default=Config.DEFAULT_WORKERS, help="Workers (not yet used)")
    p.add_argument("--max-requests", type=int, default=Config.MAX_REQUESTS_PER_RUN, 
                   help="Max requests per run")
    args = p.parse_args()
    
    tracker.max_requests = args.max_requests
    
    run_scraper(
        domains_map=Config.TARGET_DOMAINS,
        out_dir=args.out,
        target_limit=args.target,
        workers=args.workers,
    )

if __name__ == "__main__":
    # Initialize logger and translator within run_scraper so they use the correct out_dir
    logger = None
    translator = None
    main()
