import requests
import json
import time
from datetime import datetime
from bs4 import BeautifulSoup
import os
import sys
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

try:
    from deep_translator import GoogleTranslator
    TRANSLATOR_AVAILABLE = True
except ImportError:
    print("ERROR: deep-translator not installed!")
    print("Please run: pip install deep-translator")
    TRANSLATOR_AVAILABLE = False

def create_session():
    """
    Create a requests session with retry logic and increased timeout.
    """
    session = requests.Session()
    
    # Configure retry strategy
    retry_strategy = Retry(
        total=5,
        backoff_factor=2,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["HEAD", "GET", "OPTIONS"]
    )
    
    adapter = HTTPAdapter(max_retries=retry_strategy)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    
    return session

def get_wayback_url(original_url, timestamp=None, session=None):
    """
    Get the Wayback Machine archived URL for a given URL.
    """
    if session is None:
        session = requests
    
    wayback_api = "https://archive.org/wayback/available"
    
    params = {'url': original_url}
    if timestamp:
        params['timestamp'] = timestamp
    
    try:
        response = session.get(wayback_api, params=params, timeout=30)
        if response.status_code == 200:
            data = response.json()
            if 'archived_snapshots' in data and 'closest' in data['archived_snapshots']:
                return data['archived_snapshots']['closest']['url']
    except Exception as e:
        print(f"Error checking Wayback Machine: {e}")
    
    return None

def translate_text(text, source_lang='uk', dest_lang='en', max_length=4500):
    """
    Translate text using Google Translate via deep-translator.
    Splits long text into chunks if needed.
    """
    if not TRANSLATOR_AVAILABLE:
        print("    ERROR: Translator not available!")
        return ""
    
    if not text or len(text.strip()) == 0:
        return ""
    
    try:
        # If text is short enough, translate directly
        if len(text) <= max_length:
            translator = GoogleTranslator(source=source_lang, target=dest_lang)
            result = translator.translate(text)
            time.sleep(0.3)
            return result if result else ""
        
        # For longer text, split into chunks
        chunks = []
        sentences = text.split('\n')
        current_chunk = ""
        
        for sentence in sentences:
            if len(current_chunk) + len(sentence) + 1 <= max_length:
                current_chunk += sentence + "\n"
            else:
                if current_chunk:
                    chunks.append(current_chunk)
                current_chunk = sentence + "\n"
        
        if current_chunk:
            chunks.append(current_chunk)
        
        # Translate each chunk
        translated_chunks = []
        for i, chunk in enumerate(chunks):
            translator = GoogleTranslator(source=source_lang, target=dest_lang)
            result = translator.translate(chunk)
            translated_chunks.append(result if result else "")
            time.sleep(0.5)
            if len(chunks) > 1:
                print(f"    Translated chunk {i+1}/{len(chunks)}")
        
        return "\n".join(translated_chunks)
    
    except Exception as e:
        print(f"    Translation error: {e}")
        try:
            time.sleep(2)
            translator = GoogleTranslator(source=source_lang, target=dest_lang)
            result = translator.translate(text[:max_length])
            return result if result else ""
        except Exception as e2:
            print(f"    Retry translation error: {e2}")
            return ""

def extract_text_from_html(html_content):
    """
    Extract main text content from HTML using BeautifulSoup.
    """
    soup = BeautifulSoup(html_content, 'html.parser')
    
    # Remove script and style elements
    for script in soup(["script", "style", "nav", "footer", "header"]):
        script.decompose()
    
    # Try to find main content area
    main_content = soup.find('article') or soup.find('main') or soup.find('div', class_='content')
    
    if main_content:
        text = main_content.get_text(separator='\n', strip=True)
    else:
        text = soup.get_text(separator='\n', strip=True)
    
    # Clean up excessive whitespace
    lines = [line.strip() for line in text.split('\n') if line.strip()]
    text = '\n'.join(lines)
    
    return text

def scrape_from_wayback(url, timestamp=None, session=None, source_lang='uk'):
    """
    Scrape text content from a URL using Wayback Machine.
    Translates title and text to English.
    """
    if session is None:
        session = create_session()
    
    wayback_url = get_wayback_url(url, timestamp, session)
    
    if not wayback_url:
        print(f"  No archive found for {url}")
        return None
    
    print(f"  Found archive: {wayback_url}")
    
    try:
        response = session.get(wayback_url, timeout=60)
        if response.status_code == 200:
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Extract title
            title_tag = soup.find('title')
            original_title = title_tag.get_text(strip=True) if title_tag else ""
            
            # Extract text
            original_text = extract_text_from_html(response.content)
            
            # Translate title and text
            print(f"  Translating title and text...")
            translated_title = translate_text(original_title, source_lang=source_lang) if original_title else ""
            translated_text = translate_text(original_text, source_lang=source_lang) if original_text else ""
            
            return {
                'url': url,
                'wayback_url': wayback_url,
                'original_title': original_title,
                'translated_title': translated_title,
                'original_text': original_text,
                'translated_text': translated_text,
                'scraped_at': datetime.now().isoformat()
            }
    except requests.exceptions.ConnectionError as e:
        print(f"  Connection error: {e}")
        print(f"  Retrying after 10 seconds...")
        time.sleep(10)
        try:
            response = session.get(wayback_url, timeout=60)
            if response.status_code == 200:
                soup = BeautifulSoup(response.content, 'html.parser')
                title_tag = soup.find('title')
                original_title = title_tag.get_text(strip=True) if title_tag else ""
                original_text = extract_text_from_html(response.content)
                
                print(f"  Translating title and text...")
                translated_title = translate_text(original_title, source_lang=source_lang) if original_title else ""
                translated_text = translate_text(original_text, source_lang=source_lang) if original_text else ""
                
                return {
                    'url': url,
                    'wayback_url': wayback_url,
                    'original_title': original_title,
                    'translated_title': translated_title,
                    'original_text': original_text,
                    'translated_text': translated_text,
                    'scraped_at': datetime.now().isoformat()
                }
        except Exception as e2:
            print(f"  Retry failed: {e2}")
    except Exception as e:
        print(f"  Error scraping {wayback_url}: {e}")
    
    return None

def save_article(article, output_file):
    """
    Append a single article to the output file.
    """
    if os.path.exists(output_file):
        try:
            with open(output_file, 'r', encoding='utf-8') as f:
                existing_articles = json.load(f)
        except:
            existing_articles = []
    else:
        existing_articles = []
    
    existing_articles.append(article)
    
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(existing_articles, f, ensure_ascii=False, indent=4)

def process_gdelt_articles(json_file, output_file, delay=3, save_every=5, source_lang='uk'):
    """
    Process a GDELT JSON file and scrape article text from Wayback Machine.
    """
    if not TRANSLATOR_AVAILABLE:
        print("ERROR: Cannot proceed without deep-translator!")
        return
    
    session = create_session()
    
    with open(json_file, 'r', encoding='utf-8') as f:
        articles = json.load(f)
    
    print(f"Processing {len(articles)} articles from {json_file}")
    print(f"Saving to file every {save_every} successful scrapes")
    print(f"Translating from {source_lang} to English")
    
    already_scraped = set()
    if os.path.exists(output_file):
        try:
            with open(output_file, 'r', encoding='utf-8') as f:
                existing = json.load(f)
                already_scraped = {a.get('url') for a in existing if 'url' in a}
            print(f"Found {len(already_scraped)} already scraped articles, skipping them...")
        except:
            pass
    
    successful = 0
    failed = 0
    skipped = 0
    buffer = []
    
    for i, article in enumerate(articles, 1):
        url = article.get('url')
        seendate = article.get('seendate')
        
        if not url:
            continue
        
        if url in already_scraped:
            skipped += 1
            print(f"[{i}/{len(articles)}] Skipping (already scraped): {url}")
            continue
        
        print(f"[{i}/{len(articles)}] Scraping: {url}")
        
        timestamp = None
        if seendate:
            try:
                timestamp = seendate.replace('T', '').replace('Z', '')
            except:
                pass
        
        result = scrape_from_wayback(url, timestamp, session, source_lang)
        
        if result:
            article_clean = {k: v for k, v in article.items() if k != 'socialimage'}
            combined = {**article_clean, **result}
            buffer.append(combined)
            successful += 1
            
            print(f"  ✓ Successfully scraped and translated ({len(result['original_text'])} chars original, {len(result['translated_text'])} chars translated)")
            
            if len(buffer) >= save_every:
                for buffered_article in buffer:
                    save_article(buffered_article, output_file)
                print(f"  💾 Saved {len(buffer)} articles to disk")
                buffer = []
        else:
            failed += 1
            print(f"  ✗ Failed to scrape")
        
        time.sleep(delay)
    
    if buffer:
        for buffered_article in buffer:
            save_article(buffered_article, output_file)
        print(f"  💾 Saved final {len(buffer)} articles to disk")
    
    print(f"\n{'='*60}")
    print(f"Summary for {json_file}:")
    print(f"  Successful: {successful}")
    print(f"  Failed: {failed}")
    print(f"  Skipped (already scraped): {skipped}")
    print(f"  Total saved in {output_file}: {successful + len(already_scraped)}")
    print(f"{'='*60}")


def get_next_file_to_process():
    """
    Find the next file that needs processing using round-robin across languages.
    Returns: tuple of (input_file, output_file, lang_code, lang_name) or None
    """
    language_configs = [
        {'directory': 'data/gdeltdata/gdelt_russian_data', 'lang_code': 'ru', 'name': 'Russian', 'output_dir': 'data/scrapped/scraped_russian'},
        {'directory': 'data/gdeltdata/gdelt_ukrainian_data', 'lang_code': 'uk', 'name': 'Ukrainian', 'output_dir': 'data/scrapped/scraped_ukrainian'},
    ]
    
    # Create output directories
    for config in language_configs:
        os.makedirs(config['output_dir'], exist_ok=True)
    
    # Build file queue with round-robin order
    file_queue = []
    for config in language_configs:
        input_dir = config['directory']
        if not os.path.exists(input_dir):
            continue
        
        files = sorted([f for f in os.listdir(input_dir) if f.endswith('.json')])
        for filename in files:
            file_queue.append({
                'filename': filename,
                'input_dir': input_dir,
                'output_dir': config['output_dir'],
                'lang_code': config['lang_code'],
                'lang_name': config['name']
            })
    
    # Reorganize queue: one file from each language, then repeat
    organized_queue = []
    max_files = 0
    for config in language_configs:
        if os.path.exists(config['directory']):
            count = len([f for f in file_queue if f['input_dir'] == config['directory']])
            max_files = max(max_files, count)
    
    for i in range(max_files):
        for config in language_configs:
            files_for_lang = [f for f in file_queue if f['input_dir'] == config['directory']]
            if i < len(files_for_lang):
                organized_queue.append(files_for_lang[i])
    
    # Find first unprocessed file
    for file_info in organized_queue:
        output_file = os.path.join(file_info['output_dir'], file_info['filename'])
        
        # Check if output file doesn't exist or is incomplete
        if not os.path.exists(output_file):
            input_file = os.path.join(file_info['input_dir'], file_info['filename'])
            return (input_file, output_file, file_info['lang_code'], file_info['lang_name'])
        
        # Check if output file is too small
        try:
            file_size = os.path.getsize(output_file)
            if file_size < 100:
                input_file = os.path.join(file_info['input_dir'], file_info['filename'])
                return (input_file, output_file, file_info['lang_code'], file_info['lang_name'])
        except:
            input_file = os.path.join(file_info['input_dir'], file_info['filename'])
            return (input_file, output_file, file_info['lang_code'], file_info['lang_name'])
    
    return None


if __name__ == "__main__":
    # Check if running in "single file mode" (for GitHub Actions)
    if len(sys.argv) > 1 and sys.argv[1] == '--single':
        # Process just one file
        next_file = get_next_file_to_process()
        
        if next_file:
            input_file, output_file, lang_code, lang_name = next_file
            
            print(f"\n{'='*60}")
            print(f"Processing SINGLE file ({lang_name}): {os.path.basename(input_file)}")
            print(f"Output: {output_file}")
            print(f"{'='*60}\n")
            
            try:
                process_gdelt_articles(input_file, output_file, delay=3, save_every=5, source_lang=lang_code)
                print(f"\n✓ Successfully completed {os.path.basename(input_file)}")
            except Exception as e:
                print(f"\n✗ Error processing {os.path.basename(input_file)}: {e}")
                sys.exit(1)
        else:
            print("✓ All files already processed!")
            sys.exit(0)
    
    else:
        # Original behavior: process all files in round-robin order
        language_configs = [
            {'directory': 'gdelt_russian_data', 'lang_code': 'ru', 'name': 'Russian', 'output_dir': 'scraped_russian'},
            {'directory': 'gdelt_ukrainian_data', 'lang_code': 'uk', 'name': 'Ukrainian', 'output_dir': 'scraped_ukrainian'},
        ]
        
        # Create output directories for each language
        for config in language_configs:
            os.makedirs(config['output_dir'], exist_ok=True)
        
        # Get all JSON files from each directory
        file_queue = []
        for config in language_configs:
            input_dir = config['directory']
            if not os.path.exists(input_dir):
                print(f"Warning: Directory '{input_dir}' not found, skipping...")
                continue
            
            files = sorted([f for f in os.listdir(input_dir) if f.endswith('.json')])
            for filename in files:
                file_queue.append({
                    'filename': filename,
                    'input_dir': input_dir,
                    'output_dir': config['output_dir'],
                    'lang_code': config['lang_code'],
                    'lang_name': config['name']
                })
        
        if not file_queue:
            print("No JSON files found in any directory!")
            exit(1)
        
        # Reorganize queue: one file from each language, then repeat
        organized_queue = []
        max_files = max(len([f for f in file_queue if f['input_dir'] == config['directory']]) 
                       for config in language_configs if os.path.exists(config['directory']))
        
        for i in range(max_files):
            for config in language_configs:
                files_for_lang = [f for f in file_queue if f['input_dir'] == config['directory']]
                if i < len(files_for_lang):
                    organized_queue.append(files_for_lang[i])
        
        print(f"Processing {len(organized_queue)} files in round-robin order (Ukrainian → Russian)")
        print(f"{'='*60}\n")
        
        for idx, file_info in enumerate(organized_queue, 1):
            filename = file_info['filename']
            input_dir = file_info['input_dir']
            output_dir = file_info['output_dir']
            lang_code = file_info['lang_code']
            lang_name = file_info['lang_name']
            
            input_file = os.path.join(input_dir, filename)
            output_file = os.path.join(output_dir, filename)
            
            print(f"\n{'='*60}")
            print(f"[{idx}/{len(organized_queue)}] Processing {lang_name}: {filename}")
            print(f"Output: {output_file}")
            print(f"{'='*60}\n")
            
            try:
                process_gdelt_articles(input_file, output_file, delay=3, save_every=5, source_lang=lang_code)
            except Exception as e:
                print(f"Error processing {filename}: {e}")
            
            time.sleep(5)
        
        print("\n✓ All files processed!")
