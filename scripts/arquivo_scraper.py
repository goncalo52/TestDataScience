import requests
import json
import time
from datetime import datetime
from bs4 import BeautifulSoup
import os
import sys
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from urllib.parse import quote
from requests.exceptions import ConnectionError

try:
    from deep_translator import GoogleTranslator
    TRANSLATOR_AVAILABLE = True
except ImportError:
    TRANSLATOR_AVAILABLE = False
    print("Warning: deep-translator not found. Translation will be skipped.")

def create_session():
    session = requests.Session()
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

def get_arquivo_pt_url(original_url, timestamp=None, session=None):
    if session is None:
        session = create_session()
    
    arquivo_api = "https://arquivo.pt/textsearch"
    
    try:
        encoded_url = quote(original_url, safe='')
        
        params = {
            'versionHistory': original_url,
            'maxItems': 20
        }
        
        response = session.get(arquivo_api, params=params, timeout=30)
        if response.status_code == 200:
            data = response.json()
            
            if 'response_items' in data and len(data['response_items']) > 0:
                items = data['response_items']
                
                if timestamp:
                    target_ts = int(timestamp)
                    closest_item = min(items, key=lambda x: abs(int(x['tstamp']) - target_ts))
                    return closest_item['linkToNoFrame']
                else:
                    return items[0]['linkToNoFrame']
    except Exception as e:
        print(f"Error checking Arquivo.pt: {e}")
    
    return None


def translate_text(text, source_lang='pt', dest_lang='en', max_length=4500, max_retries=3):
    if not TRANSLATOR_AVAILABLE or not text or len(text.strip()) == 0:
        return ""
    
    translator = GoogleTranslator(source=source_lang, target=dest_lang)
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
        
    attempt = 0
    while attempt < max_retries:
        translated_chunks = []
        all_chunks_translated = True
        try:
            for i, chunk in enumerate(chunks):
                result = translator.translate(chunk)
                
                if not result:
                    print(f"    Warning: Chunk {i+1}/{len(chunks)} returned empty result. Retrying...")
                    all_chunks_translated = False
                    break  
                translated_chunks.append(result)
                time.sleep(0.5) 
                
                if len(chunks) > 1 and attempt == 0:
                    print(f"    Translated chunk {i+1}/{len(chunks)}")
            
            if all_chunks_translated:
                return "\n".join(translated_chunks)

        except Exception as e:
            print(f"    Translation error on attempt {attempt + 1}: {e}")
            all_chunks_translated = False

        attempt += 1
        if not all_chunks_translated and attempt < max_retries:
            wait_time = 2 ** attempt
            print(f"    Waiting {wait_time} seconds before retrying...")
            time.sleep(wait_time)
        
    print(f"    ERROR: Failed to translate text after {max_retries} attempts.")
    return ""

def _extract_and_translate(response_content, url, arquivo_url, source_lang,min_words=150):
    soup = BeautifulSoup(response_content, 'html.parser')
    
    title_tag = soup.find('title')
    original_title = title_tag.get_text(strip=True) if title_tag else ""
    
    original_text = extract_text_from_html(response_content)
    word_count = len(original_text.split())
    if word_count < min_words:
        print(f"  Skipping article: Only {word_count} words found (below minimum of {min_words}).")
        return None 
    print(f"  Translating title and text...")
    translated_title = translate_text(original_title, source_lang=source_lang) if original_title else ""
    translated_text = translate_text(original_text, source_lang=source_lang) if original_text else ""
    
    return {
        'url': url,
        'arquivo_pt_url': arquivo_url,
        'original_title': original_title,
        'translated_title': translated_title,
        'original_text': original_text,
        'translated_text': translated_text,
        'scraped_at': datetime.now().isoformat()
    }


def extract_text_from_html(html_content):
    soup = BeautifulSoup(html_content, 'html.parser')
    
    for script in soup(["script", "style"]):
        script.decompose()
    
    main_content = soup.find('article') or soup.find('main') or soup.find('div', class_='content')    
    
    if main_content:
        text = main_content.get_text(separator='\n', strip=True)
    else:
        text = soup.get_text(separator='\n', strip=True)
    
    lines = [line.strip() for line in text.split('\n') if line.strip()]
    text = '\n'.join(lines)
    
    return text

def scrape_from_arquivo_pt(url, timestamp=None, session=None, source_lang='pt'):
    if session is None:
        session = create_session()
    
    arquivo_url = get_arquivo_pt_url(url, timestamp, session)
    
    if not arquivo_url:
        print(f"  No archive found for {url}")
        return None
    
    print(f"  Found archive: {arquivo_url}")
    
    try:
        response = session.get(arquivo_url, timeout=60)
        response.raise_for_status()
        
        return _extract_and_translate(response.content, url, arquivo_url, source_lang)
        
    except ConnectionError as e:
        print(f"  Connection error: {e}")
        print(f"  Retrying after 10 seconds...")
        time.sleep(10)
        try:
            response = session.get(arquivo_url, timeout=60)
            response.raise_for_status()
            return _extract_and_translate(response.content, url, arquivo_url, source_lang)
            
        except Exception as e2:
            print(f"  Retry failed: {e2}")
            
    except Exception as e:
        print(f"  Error scraping {arquivo_url}: {e}")
    
    return None

def save_article(article, output_file):
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

def process_gdelt_articles(json_file, output_file, delay=3, save_every=5, source_lang='pt'):
    if not TRANSLATOR_AVAILABLE:
        print("ERROR: Cannot proceed without deep-translator!")
        return
    
    session = create_session()
    
    with open(json_file, 'r', encoding='utf-8') as f:
        articles = json.load(f)
    
    print(f"Processing {len(articles)} articles from {json_file}")
    print(f"Saving to file every {save_every} successful scrapes")
    print(f"Translating from {source_lang} to English")
    print(f"Using Arquivo.pt archive")
    
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
        
        result = scrape_from_arquivo_pt(url, timestamp, session, source_lang)
        
        if result:
            keys_to_exclude = ['url_mobile']
            article_clean = {k: v for k, v in article.items() if k not in keys_to_exclude}
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


def get_next_file_to_process(input_directory, output_directory):
    """Find the next file that needs processing"""
    if not os.path.exists(input_directory):
        return None
    
    input_files = sorted([f for f in os.listdir(input_directory) if f.endswith('.json')])
    
    for filename in input_files:
        output_file = os.path.join(output_directory, filename)
                if not os.path.exists(output_file):
            return filename
    
    return None


if __name__ == "__main__":
    input_directory = 'data/gdeltdata/gdelt_portuguese_data'
    output_directory = 'data/scrapped/scrapped_portuguese'
    
    os.makedirs(output_directory, exist_ok=True)
    
    if len(sys.argv) > 1 and sys.argv[1] == '--single':
        next_file = get_next_file_to_process(input_directory, output_directory)
        
        if next_file:
            input_file = os.path.join(input_directory, next_file)
            output_file = os.path.join(output_directory, next_file)
            
            print(f"\n{'='*60}")
            print(f"Processing SINGLE file: {next_file}")
            print(f"Output: {output_file}")
            print(f"{'='*60}\n")
            
            try:
                process_gdelt_articles(input_file, output_file, delay=3, save_every=5, source_lang='pt')
                print(f"\n✓ Successfully completed {next_file}")
            except Exception as e:
                print(f"\n✗ Error processing {next_file}: {e}")
                sys.exit(1)
        else:
            print("✓ All files already processed!")
            sys.exit(0)
    
    else:
        if not os.path.exists(input_directory):
            print(f"Error: Input directory not found: {input_directory}")
        else:
            for filename in sorted(os.listdir(input_directory)):
                if filename.endswith('.json'):
                    input_file = os.path.join(input_directory, filename)
                    output_file = os.path.join(output_directory, filename)
                    
                    print(f"\n{'='*60}")
                    print(f"Processing: {filename}")
                    print(f"Output: {output_file}")
                    print(f"{'='*60}\n")
                    
                    try:
                        process_gdelt_articles(input_file, output_file, delay=3, save_every=5, source_lang='pt')
                    except Exception as e:
                        print(f"Error processing {filename}: {e}")
                    
                    time.sleep(5)
            
            print("\n✓ All Portuguese files processed using Arquivo.pt!")
