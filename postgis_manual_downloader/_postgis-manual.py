#!/usr/bin/env python3

"""
PostGIS Manual Processor: Downloads and combines the PostGIS manual.
Assumes it's run via the python interpreter in the .venv_pg_manual created by install.sh
"""

import sys

# Ensure correct Python version is used (safeguard)
if sys.version_info < (3, 12):
    # This error is less likely now since install.sh tries to create the venv with 3.12
    # but it's a good check if venv creation fell back to another version.
    sys.exit(f"Error: This script requires Python 3.12 or later. Found: {sys.version}")

try:
    import hashlib
    import requests
    from pathlib import Path
    from urllib.parse import urljoin, urlparse
    from bs4 import BeautifulSoup, SoupStrainer
    import lxml
except ImportError as e:
    # This error indicates install.sh failed to install dependencies correctly.
    print(f"Error: Required Python package missing: {e}", file=sys.stderr)
    print("This might indicate an issue during the dependency installation.", file=sys.stderr)
    print("Try running 'install.sh' again.", file=sys.stderr)
    sys.exit(1)

# Configuration
BASE_URL = "https://postgis.net/docs/manual-dev/en/"
OUTPUT_FILE = "postgis_manual_single.html"
# Use a subdirectory within the script's location for downloads
DOWNLOAD_DIR = Path(__file__).parent / "postgis_docs_download"
MAX_DEPTH = 3 # How many levels of links to follow and inline
EXCLUDE_SELECTORS = [
    '.navheader', '.navfooter', 'img[alt="Edit this page"]', 'script',
    'link[rel="stylesheet"]', 'table.nav', '.editsection', 'a.ulink' # Exclude external links explicitly if needed
]
# CSS to make the final document somewhat readable
CSS_OVERRIDE = """
<style>
  body { font-family: sans-serif; line-height: 1.6; max-width: 900px; margin: 20px auto; padding: 0 15px; }
  h1, h2, h3, h4 { color: #333; }
  h1 { border-bottom: 2px solid #eee; padding-bottom: 10px; }
  pre { background-color: #f4f4f4; border: 1px solid #ddd; padding: 10px; overflow-x: auto; white-space: pre-wrap; word-wrap: break-word; }
  code { background-color: #f4f4f4; padding: 2px 4px; border-radius: 3px; }
  pre code { background-color: transparent; padding: 0; border-radius: 0; }
  a { color: #007bff; text-decoration: none; }
  a:hover { text-decoration: underline; }
  .toc { border: 1px solid #ccc; padding: 15px; margin-bottom: 20px; background-color: #f9f9f9; }
  .toc h1 { font-size: 1.5em; margin-top: 0; border-bottom: none; }
  .toc ul { padding-left: 20px; }
  .expanded-content {
      margin-left: 20px;
      border-left: 3px solid #e0e0e0;
      padding-left: 15px;
      margin-top: 15px;
      margin-bottom: 15px;
      background-color: #fafafa; /* Slight background change */
      padding-top: 10px;
      padding-bottom: 10px;
  }
  .source-link {
      color: #555;
      font-size: 0.85em;
      margin-bottom: 10px;
      display: block;
      font-style: italic;
  }
</style>
"""

processed_urls = set()
downloaded_files = {} # Store URL -> Path mapping
internal_domain = urlparse(BASE_URL).netloc

def setup_environment():
    """Create download directory if it doesn't exist."""
    DOWNLOAD_DIR.mkdir(parents=True, exist_ok=True)
    print(f"Using download cache directory: {DOWNLOAD_DIR}")

def generate_filename(url):
    """Create unique, safe filename from URL using MD5 hash."""
    parsed = urlparse(url)
    path_key = parsed.path.strip('/') or "index"
    if parsed.query:
        path_key += "?" + parsed.query
    if parsed.fragment:
        path_key += "#" + parsed.fragment
    safe_hash = hashlib.md5(path_key.encode('utf-8')).hexdigest()[:16]
    name_part = Path(parsed.path).stem or "page"
    safe_name = "".join(c if c.isalnum() else '_' for c in name_part)[:30]
    return f"{safe_name}_{safe_hash}.html"

def is_internal_link(base_url, href):
    """Check if link points to documentation content within the same base path."""
    if not href or href.startswith('#') or href.startswith('mailto:') or href.startswith('javascript:'):
        return False
    abs_url = urljoin(base_url, href)
    parsed_abs = urlparse(abs_url)
    if parsed_abs.netloc and parsed_abs.netloc != internal_domain:
        return False
    base_path = urlparse(BASE_URL).path
    if not parsed_abs.path.startswith(base_path):
        return False
    return True

def download_resource(url):
    """Download resource if not already downloaded. Returns file path or None."""
    if url in downloaded_files:
        return downloaded_files[url]
    filepath = DOWNLOAD_DIR / generate_filename(url)
    if filepath.exists():
        print(f"Cache hit: Using existing file for {url}")
        downloaded_files[url] = filepath
        return filepath

    print(f"Downloading: {url}")
    try:
        response = requests.get(url, timeout=20)
        response.raise_for_status()
        if 'text/html' not in response.headers.get('Content-Type', ''):
            print(f"Warning: Skipping non-HTML content at {url} ({response.headers.get('Content-Type')})", file=sys.stderr)
            return None
        content = response.text
        if not content.strip().startswith('<') or not content.strip().endswith('>'):
             print(f"Warning: Content from {url} doesn't look like HTML. Skipping.", file=sys.stderr)
             return None
        with open(filepath, 'w', encoding='utf-8') as f:
            f.write(content)
        downloaded_files[url] = filepath
        return filepath
    except requests.exceptions.RequestException as e:
        print(f"Error downloading {url}: {e}", file=sys.stderr)
        return None
    except Exception as e:
        print(f"Error processing download for {url}: {e}", file=sys.stderr)
        return None

def get_toc_structure(base_url):
    """Extract table of contents structure from the main index page."""
    index_url = urljoin(base_url, "index.html")
    print(f"Fetching Table of Contents from: {index_url}")
    filepath = download_resource(index_url)
    if not filepath:
        sys.exit("Error: Could not download the main index page. Cannot build TOC.")

    toc = []
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            strainer = SoupStrainer('div', class_='toc')
            soup = BeautifulSoup(f.read(), 'lxml', parse_only=strainer)
        toc_div = soup.find('div', class_='toc')
        if not toc_div:
             print("Warning: Could not find <div class='toc'> in index.html.", file=sys.stderr)
             return []
        for item in toc_div.select(':scope > ul > li, :scope > ol > li'):
            link = item.find('a', href=True)
            if link:
                href = link['href']
                if not href or href.startswith('#'):
                    continue
                abs_url = urljoin(index_url, href)
                if is_internal_link(index_url, href):
                    title = link.get_text(strip=True) or f"Untitled Section ({href})"
                    toc.append({"url": abs_url, "title": title, "id": f"section_{len(toc) + 1}"})
    except Exception as e:
        print(f"Error parsing TOC from {filepath}: {e}", file=sys.stderr)
        return []
    if not toc:
         print("Warning: No TOC entries found. Check TOC selectors and index page structure.", file=sys.stderr)
    return toc

def process_content(url, depth=0):
    """Recursively process HTML content, expanding internal links."""
    if url in processed_urls: return None
    if depth > MAX_DEPTH:
        print(f"Max depth ({MAX_DEPTH}) reached, not expanding: {url}")
        return None

    processed_urls.add(url)
    filepath = downloaded_files.get(url)
    if not filepath or not filepath.exists():
        print(f"Warning: File path not found or doesn't exist for {url}. Skipping processing.", file=sys.stderr)
        return None

    print(f"Processing (depth {depth}): {url}")
    try:
        with open(filepath, 'r', encoding='utf-8') as f: content = f.read()
        strainer = SoupStrainer(['div', 'section'], attrs={'role': 'main', 'class': ['chapter', 'refentry', 'sect1', 'article']})
        soup = BeautifulSoup(content, 'lxml', parse_only=strainer)
        main_content = soup.find(['div', 'section'], attrs={'role': 'main'}) \
                    or soup.find(['div', 'section'], class_='chapter') \
                    or soup.find(['div', 'section'], class_='refentry') \
                    or soup.find(['div', 'section'], class_='sect1') \
                    or soup.find(['div', 'section'], class_='article') \
                    or soup.body
        if not main_content:
            print(f"Warning: Could not find main content container in {url}. Processing full body.", file=sys.stderr)
            soup_full = BeautifulSoup(content, 'lxml')
            main_content = soup_full.body or soup_full
            if not main_content: return None

        elements_to_remove = []
        for selector in EXCLUDE_SELECTORS:
            try: elements_to_remove.extend(main_content.select(selector))
            except Exception as e: print(f"Warning: Error selecting '{selector}' in {url}: {e}", file=sys.stderr)
        for element in set(elements_to_remove):
             if element and element.parent: element.decompose()

        links_to_process = main_content.find_all('a', href=True)
        for link in links_to_process:
            if not link.parent: continue
            href = link['href']
            if not is_internal_link(url, href): continue
            abs_url = urljoin(url, href)
            current_url_base = url.split('#')[0]
            target_url_base = abs_url.split('#')[0]
            if current_url_base == target_url_base and '#' in abs_url: continue

            linked_filepath = download_resource(abs_url)
            if not linked_filepath: continue

            processed_linked_content = process_content(abs_url, depth + 1)
            if processed_linked_content:
                # Must create tags using the soup object from *this* level of recursion
                wrapper_soup = BeautifulSoup('', 'lxml') # Need a factory for new tags
                wrapper = wrapper_soup.new_tag('div', **{'class': 'expanded-content'})
                source_note = wrapper_soup.new_tag('div', **{'class': 'source-link'})
                source_note.string = f"↪ Content from: {abs_url}"
                wrapper.append(source_note)
                # Append the processed content (which is a Tag object)
                # We need to parse the string representation if it came from a different soup
                wrapper.append(BeautifulSoup(str(processed_linked_content), 'lxml').contents[0])

                parent = link.parent
                try:
                    if parent and parent.name in ['p', 'li'] and len(parent.get_text(strip=True)) == len(link.get_text(strip=True)):
                        parent.replace_with(wrapper)
                    else:
                        link.replace_with(wrapper)
                except Exception as replace_err:
                     print(f"Warning: Could not replace link/parent with wrapper for {abs_url} in {url}: {replace_err}", file=sys.stderr)
                     # Fallback: append after link if replace fails
                     try: link.insert_after(wrapper)
                     except: pass # Ignore if insert fails too

        return main_content # Return the modified Tag object
    except FileNotFoundError:
        print(f"Error: File not found during processing: {filepath}", file=sys.stderr)
        return None
    except Exception as e:
        print(f"Error processing content of {url} from {filepath}: {e}", file=sys.stderr)
        error_soup = BeautifulSoup("", "lxml")
        error_div = error_soup.new_tag('div', style="color: red; border: 1px solid red; padding: 10px;")
        error_div.string = f"[Error processing content from {url}: {e}]"
        return error_div # Return a placeholder tag

def build_manual(toc):
    """Construct the final single HTML document from processed content."""
    final_soup = BeautifulSoup("<!DOCTYPE html><html><head><meta charset='UTF-8'><title>PostGIS Manual (Combined)</title></head><body></body></html>", 'html.parser')
    head, body = final_soup.head, final_soup.body
    style_tag = final_soup.new_tag('style'); style_tag.string = CSS_OVERRIDE; head.append(style_tag)

    toc_container = final_soup.new_tag('div', **{'class': 'toc'})
    toc_title = final_soup.new_tag('h1'); toc_title.string = "PostGIS Manual - Table of Contents"; toc_container.append(toc_title)
    toc_list = final_soup.new_tag('ul')
    for entry in toc:
        item = final_soup.new_tag('li'); link = final_soup.new_tag('a', href=f"#{entry['id']}"); link.string = entry['title']; item.append(link); toc_list.append(item)
    toc_container.append(toc_list); body.append(toc_container)

    processed_urls.clear() # Reset for the main build phase
    for entry in toc:
        url, title, section_id = entry['url'], entry['title'], entry['id']
        content_tag = process_content(url, depth=0) # Start recursion
        if content_tag:
            header = final_soup.new_tag('h1', id=section_id); header.string = title; body.append(header)
            # Append the processed tag, ensuring it belongs to *this* soup instance
            body.append(BeautifulSoup(str(content_tag), 'lxml').contents[0])
            body.append(final_soup.new_tag('hr'))
        else:
            print(f"Warning: No content generated for TOC entry: {title} ({url})", file=sys.stderr)
            missing_header = final_soup.new_tag('h1', id=section_id); missing_header.string = title; body.append(missing_header)
            missing_note = final_soup.new_tag('p', style="color: orange;"); missing_note.string = f"[Content for this section could not be processed or was empty]"; body.append(missing_note)
            body.append(final_soup.new_tag('hr'))
    return final_soup.prettify()

def main():
    """Main execution function."""
    setup_environment()
    toc = get_toc_structure(BASE_URL)
    if not toc: sys.exit("Failed to build Table of Contents. Exiting.")
    print(f"Found {len(toc)} top-level TOC entries.")
    print("Pre-downloading main chapter pages listed in TOC...")
    toc_urls = {entry['url'] for entry in toc}
    for url in toc_urls: download_resource(url)
    print("Processing chapters and expanding internal links (up to MAX_DEPTH)...")
    manual_html = build_manual(toc)
    try:
        with open(OUTPUT_FILE, 'w', encoding='utf-8') as f: f.write(manual_html)
        print(f"\nSuccess! Combined manual saved to: {OUTPUT_FILE}")
    except IOError as e:
        print(f"\nError writing final output file {OUTPUT_FILE}: {e}", file=sys.stderr)
        sys.exit(1)

if __name__ == "__main__":
    # This check is still useful if the script is somehow invoked directly
    # with python instead of through the shebang mechanism.
    main()
