#!/usr/bin/env python3

import sys

if sys.version_info < (3, 13):
    sys.exit(f"Error: This script requires Python 3.13 or later. Found: {sys.version}")

import logging
import hashlib
import httpx
import re
import copy
from pathlib import Path
from urllib.parse import urlparse, urldefrag, urljoin
from bs4 import BeautifulSoup, Tag, NavigableString, Comment, ProcessingInstruction
from lxml import html as lxml_html, etree
from datetime import datetime

SCRIPT_DIR = Path(__file__).parent.resolve()
URL_LIST_FILE: Path = SCRIPT_DIR / "urls.txt"
OUTPUT_DIR: Path = SCRIPT_DIR / "_output"
REQUEST_TIMEOUT: int = 30
USER_AGENT: str = "URLContentCleaner/2.0 (+https://github.com/your-repo/url-content-cleaner)" # Updated version

LLM_OPTIMIZED_TAGS: set[str] = {
    "html", "head", "title", "body",
    "main", "article", "section",
    "h1", "h2", "h3", "h4", "h5", "h6", "p", "br", "hr",
    "ul", "ol", "li", "dl", "dt", "dd",
    "strong", "em", "b", "i", "code", "pre", "mark",
    "blockquote", "q", "cite",
    "table", "caption", "thead", "tbody", "tr", "th", "td",
    "a"
}

ATTRIBUTE_ALLOW_LIST: dict[str, set[str]] = {
    "a": {"href"},
    "pre": {"class"},
    "code": {"class"},
    "th": {"scope"},
    "td": {"colspan", "rowspan"},
    "html": {"lang"},
    "blockquote": {"cite"},
    "q": {"cite"},
}

NON_CONTENT_TAGS: set[str] = {
    "script", "style", "link", "meta",
    "img", "picture", "figure", "figcaption", "svg", "canvas", "video", "audio", "iframe",
    "nav", "aside", "header", "footer",
    "form", "input", "button", "select", "textarea", "label",
    "noscript", "template", "map", "area", "object", "embed", "source"
}

NON_CONTENT_PATTERNS: dict[str, list[str]] = {
    "class": [
        "nav", "menu", "sidebar", "footer", "header", "banner", "cookie", "modal",
        "popup", "social", "comment", "related", "widget", "ads", "pagination",
        "breadcrumb", "toolbar", "utility", "skip-link", "site-header", "site-footer",
        "advertisement", "promo", "subnav", "flyout", "dropdown", "toc", "index"
    ],
    "id": [
        "navigation", "menu", "sidebar", "footer", "header", "banner", "cookie",
        "modal", "popup", "social", "comments", "related", "widget", "ads", "pagination",
        "breadcrumbs", "toolbar", "utilities", "skip", "siteheader", "sitefooter",
        "advert", "promo", "subnav", "flyout", "dropdown", "toc", "index"
    ],
    "role": [
        "navigation", "banner", "contentinfo", "complementary", "search", "form",
        "dialog", "alertdialog", "menubar", "toolbar", "directory", "log", "status"
    ]
}

BOILERPLATE_PHRASES: set[str] = {
    "cookie preferences", "privacy policy", "terms of service", "terms of use",
    "all rights reserved", "related articles", "share this", "follow us",
    "skip to main content", "advertisement", "subscribe now", "sign up", "log in",
    "register", "download pdf", "print page", "add to cart", "view cart",
    "customer service", "contact us", "site map", "about us", "careers"
}

MIN_MAIN_CONTENT_LENGTH: int = 150
MIN_CANDIDATE_TEXT_LENGTH: int = 100
MAX_LINK_DENSITY_FOR_CONTENT: float = 0.65
MAX_BOILERPLATE_PHRASE_LENGTH: int = 250

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(filename)s:%(lineno)d - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[logging.StreamHandler(sys.stdout)],
)


def setup_environment():
    try:
        OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
        logging.info(f"Ensured output directory exists: {OUTPUT_DIR}")
    except OSError as e:
        logging.error(f"Failed to create output directory {OUTPUT_DIR}: {e}")
        sys.exit(1)

def load_urls(filepath: Path) -> list[str]:
    urls: list[str] = []
    if not filepath.is_file():
        logging.error(f"URL list file not found: {filepath}")
        return urls
    try:
        with filepath.open("r", encoding="utf-8") as f:
            for i, line in enumerate(f, 1):
                stripped_line = line.strip()
                if stripped_line and not stripped_line.startswith("#"):
                    urls.append(stripped_line)
        logging.info(f"Loaded {len(urls)} URLs from {filepath}")
    except IOError as e:
        logging.error(f"Failed to read URL list file {filepath}: {e}")
    return urls

def generate_filename(url: str) -> str:
    url_hash = hashlib.sha256(url.encode('utf-8')).hexdigest()
    url_no_frag, _ = urldefrag(url)
    parsed = urlparse(url_no_frag)
    domain = parsed.netloc.replace(".", "_").replace("-", "_")
    path_part = (parsed.path.strip('/') or "index").replace("/", "_").replace(":", "_").replace("-", "_")
    name_part = f"{domain}_{path_part}"
    safe_name = "".join(c if c.isalnum() or c == '_' else '' for c in name_part).strip('_')
    safe_name = re.sub(r'_+', '_', safe_name)[:100]
    short_hash = url_hash[:16]
    return f"{safe_name}_{short_hash}.html"

def fetch_url_content(url: str, client: httpx.Client) -> str | None:
    logging.info(f"Fetching: {url}")
    try:
        response = client.get(url, follow_redirects=True, timeout=REQUEST_TIMEOUT)
        response.raise_for_status()
        content_type = response.headers.get("content-type", "").lower()
        if "text/html" not in content_type:
            logging.warning(f"Skipping non-HTML content ({content_type}) at: {url}")
            return None

        detected_encoding = response.encoding or response.charset_encoding or "utf-8"
        try:
            html_content = response.content.decode(detected_encoding, errors='replace')
        except Exception as decode_err:
             logging.warning(f"Decoding issue with {detected_encoding} for {url}, trying utf-8 fallback: {decode_err}")
             html_content = response.content.decode("utf-8", errors='replace')

        if not html_content or html_content.isspace():
            logging.warning(f"Fetched empty or whitespace-only content from: {url}")
            return None
        return html_content
    except httpx.HTTPStatusError as e:
        logging.error(f"HTTP error fetching {url}: {e.response.status_code} {e.response.reason_phrase}")
    except httpx.RequestError as e:
        logging.error(f"Network error fetching {url}: {e}")
    except Exception as e:
        logging.error(f"Unexpected error fetching {url}: {e}")
    return None

def is_likely_boilerplate(element: Tag) -> bool:
    if not isinstance(element, Tag):
        return False

    tag_name = element.name.lower()
    if tag_name in ['nav', 'aside', 'header', 'footer', 'form', 'select', 'option', 'button', 'input', 'textarea', 'label']:
        return True

    for attr_name, patterns in NON_CONTENT_PATTERNS.items():
        attr_value = element.get(attr_name)
        if not attr_value:
            continue

        values = attr_value if isinstance(attr_value, list) else [attr_value]
        values_lower = [v.lower() for v in values]

        if any(pattern in val for pattern in patterns for val in values_lower):
            logging.debug(f"Boilerplate detected: Attribute '{attr_name}' match for element <{tag_name}>")
            return True

    text_content = element.get_text(" ", strip=True)
    text_length = len(text_content)
    link_count = len(element.find_all('a', recursive=False)) # Consider direct links first

    if text_length == 0 and link_count == 0 and not element.find_all(recursive=False):
        return True # Empty non-semantic container

    if link_count > 2:
        link_text_length = sum(len(a.get_text(strip=True)) for a in element.find_all('a'))
        if text_length > 0:
            link_text_ratio = link_text_length / text_length
            if link_text_ratio > MAX_LINK_DENSITY_FOR_CONTENT and text_length < MAX_BOILERPLATE_PHRASE_LENGTH * 2:
                 logging.debug(f"Boilerplate detected: High link density ({link_text_ratio:.2f}) in short element <{tag_name}>")
                 return True

    text_lower = text_content.lower()
    if text_length > 0 and text_length < MAX_BOILERPLATE_PHRASE_LENGTH:
         if any(phrase in text_lower for phrase in BOILERPLATE_PHRASES):
             logging.debug(f"Boilerplate detected: Common phrase match in short element <{tag_name}>")
             return True

    return False

def extract_main_content(soup: BeautifulSoup) -> Tag:
    logging.debug("Attempting to extract main content area...")
    potential_main_content: Tag | None = None

    # Strategy 1: Semantic Tags and Common IDs/Classes
    selectors = ['main', 'article', '[role="main"]', '#content', '#main-content', '.content', '.main-content',
                 '#bodyContent', '.post-content', '.entry-content', '.article-body', '.story-content']
    for selector in selectors:
        candidate = soup.select_one(selector)
        if candidate and len(candidate.get_text(strip=True)) > MIN_MAIN_CONTENT_LENGTH and not is_likely_boilerplate(candidate):
            logging.info(f"Main content found using selector: '{selector}'")
            potential_main_content = candidate
            break

    if potential_main_content:
        return potential_main_content

    logging.debug("Semantic/common selectors failed, trying text density analysis...")
    # Strategy 2: Text Density Analysis (Simplified)
    candidates: dict[Tag, float] = {}
    for container in soup.find_all(['div', 'section', 'td'], recursive=True): # Include td for table-based layouts
        if is_likely_boilerplate(container):
            continue

        text_length = len(container.get_text(strip=True))
        if text_length < MIN_CANDIDATE_TEXT_LENGTH:
            continue

        # Penalize containers with many boilerplate children
        child_penalty = sum(1 for child in container.find_all(recursive=False) if isinstance(child, Tag) and is_likely_boilerplate(child))
        score = text_length / (1 + child_penalty)

        # Check if candidate is nested inside another good candidate
        is_nested_in_better = False
        parent = container.parent
        while parent and parent != soup:
             if parent in candidates and candidates[parent] > score * 1.1: # Parent is significantly better
                 is_nested_in_better = True
                 break
             parent = parent.parent

        if not is_nested_in_better:
             # Avoid adding containers that are just wrappers for already selected better candidates
             should_add = True
             children_to_remove = []
             for existing_candidate in list(candidates.keys()):
                 if container.find(existing_candidate): # Current container wraps an existing candidate
                     if score < candidates[existing_candidate] * 1.1: # Existing is better or similar
                         should_add = False
                         break
                     else: # Current container is better, remove the nested one
                          children_to_remove.append(existing_candidate)
                 elif existing_candidate.find(container): # Current container is inside an existing candidate
                      if candidates[existing_candidate] > score * 1.1: # Existing is better
                          should_add = False
                          break

             if should_add:
                  for child in children_to_remove:
                       del candidates[child]
                  candidates[container] = score
                  logging.debug(f"Density candidate: <{container.name}> Score: {score:.2f}, Text Length: {text_length}")


    if candidates:
        best_candidate = max(candidates.items(), key=lambda item: item[1])[0]
        logging.info(f"Main content identified using text density: <{best_candidate.name}> with score {candidates[best_candidate]:.2f}")
        return best_candidate

    logging.warning("Could not identify a specific main content area, falling back to <body>.")
    return soup.body or soup # Fallback


def strip_undesirables(soup: BeautifulSoup) -> BeautifulSoup:
    logging.debug("Stripping undesirable elements (comments, hidden, non-content tags)...")
    # Remove comments and processing instructions
    for element in soup.find_all(text=lambda text: isinstance(text, (Comment, ProcessingInstruction))):
        element.extract()

    # Remove elements explicitly marked as hidden
    for element in soup.find_all(attrs={"hidden": True}):
        element.decompose()
    for element in soup.select('[style*="display: none"], [style*="display:none"], [style*="visibility: hidden"]'):
        element.decompose()
    for element in soup.find_all(attrs={"aria-hidden": "true"}):
         # Be slightly more careful with aria-hidden, might contain screen-reader only text
         if not element.get_text(strip=True):
              element.decompose()

    # Remove predefined non-content tags
    for tag_name in NON_CONTENT_TAGS:
        for tag in soup.find_all(tag_name):
            tag.decompose()

    return soup


def clean_element_recursively(element: Tag | NavigableString, original_url: str, depth: int = 0, max_depth: int = 30) -> Tag | NavigableString | None:
    if depth > max_depth:
        logging.warning(f"Recursion depth limit ({max_depth}) reached, skipping further nesting.")
        return None

    if isinstance(element, NavigableString):
        # Normalize whitespace later, keep string for now if not empty
        stripped = element.string.strip()
        return NavigableString(stripped) if stripped else None

    if not isinstance(element, Tag):
        return None # Should not happen with BS4 elements

    tag_name = element.name.lower()

    # If tag is not allowed, try to unwrap its children
    if tag_name not in LLM_OPTIMIZED_TAGS:
        logging.debug(f"Unwrapping disallowed tag: <{tag_name}>")
        # Create a fragment to hold unwrapped children
        fragment = BeautifulSoup('', 'lxml').new_tag('div') # Temporary wrapper
        for child in element.children:
            cleaned_child = clean_element_recursively(child, original_url, depth + 1, max_depth)
            if cleaned_child:
                fragment.append(cleaned_child)
        # Return the temporary wrapper, its contents will be moved by the caller
        return fragment if fragment.contents else None


    # Tag is allowed, create a clean copy
    clean_tag = BeautifulSoup('', 'lxml').new_tag(element.name) # Use original case

    # Clean attributes
    allowed_attrs_for_tag = ATTRIBUTE_ALLOW_LIST.get(tag_name, set())
    for attr, value in element.attrs.items():
        attr_lower = attr.lower()
        if attr_lower in allowed_attrs_for_tag:
            cleaned_value = value
            if tag_name == "a" and attr_lower == "href":
                try:
                    # Attempt to resolve to absolute URL
                    absolute_href = urljoin(original_url, str(value).strip())
                    parsed_href = urlparse(absolute_href)
                    if parsed_href.scheme in ('http', 'https'):
                        cleaned_value = absolute_href
                    else:
                        # Skip non-http(s) URLs or data URIs etc.
                        logging.debug(f"Skipping non-http(s) href '{absolute_href}' resolved from '{value}'")
                        continue
                except ValueError as e:
                   logging.warning(f"Could not parse/resolve href '{value}' from {original_url}: {e}")
                   continue # Skip invalid hrefs
                except Exception as e:
                   logging.warning(f"Error processing href '{value}' from {original_url}: {e}")
                   continue
            elif tag_name in ["pre", "code"] and attr_lower == "class":
                 # Keep only language-related classes
                 classes = value if isinstance(value, list) else str(value).split()
                 lang_class = next((c for c in classes if c.lower().startswith(("language-", "lang-"))), None)
                 cleaned_value = [lang_class] if lang_class else []
                 if not cleaned_value:
                     continue # Skip empty class attribute
            elif isinstance(cleaned_value, list):
                cleaned_value = [str(v) for v in cleaned_value]
            else:
                cleaned_value = str(cleaned_value)

            if cleaned_value: # Ensure value is not empty after cleaning
                 clean_tag[attr] = cleaned_value

    # Recursively clean children
    for child in element.children:
        cleaned_child = clean_element_recursively(child, original_url, depth + 1, max_depth)
        if cleaned_child:
            # If child was unwrapped, append its contents
            if isinstance(cleaned_child, Tag) and cleaned_child.name == 'div' and not cleaned_child.attrs:
                for unwrapped_content in list(cleaned_child.contents):
                    clean_tag.append(unwrapped_content)
            else:
                clean_tag.append(cleaned_child)

    # Discard the cleaned tag if it's become empty (unless it's a self-closing tag like br/hr)
    if not clean_tag.contents and not isinstance(clean_tag, NavigableString) and tag_name not in ['br', 'hr']:
        # Also check if it only contains whitespace strings
        if all(isinstance(c, NavigableString) and not c.string.strip() for c in clean_tag.contents):
            logging.debug(f"Removing empty or whitespace-only tag: <{tag_name}>")
            return None

    return clean_tag


def simplify_tables(soup: BeautifulSoup):
    logging.debug("Simplifying table structures...")
    for table in soup.find_all('table'):
        # Basic structure is kept by ALLOWED_TAGS and clean_element_recursively
        # This function could add more specific table logic if needed,
        # e.g., converting complex tables to simpler formats or adding summaries.
        # For now, rely on the generic cleaning.
        pass
    return soup

def preserve_code_blocks(soup: BeautifulSoup):
    logging.debug("Preserving formatting in code blocks...")
    for pre in soup.find_all('pre'):
        # The recursive cleaner already handled class attributes.
        # Ensure whitespace inside is treated literally.
        # Re-getting text might be necessary if internal tags were modified
        # But clean_element_recursively should handle this reasonably.
        # We might re-wrap content in a single string if needed, but try without first.
        pass # Rely on recursive cleaner preserving NavigableStrings within pre

    for code in soup.find_all('code'):
        # Same logic as pre, rely on recursive cleaner.
        pass
    return soup


def normalize_text_content(soup: BeautifulSoup) -> BeautifulSoup:
    logging.debug("Normalizing text content and merging paragraphs...")
    # Consolidate whitespace within text nodes
    for text_node in soup.find_all(text=True):
        if isinstance(text_node, NavigableString):
            parent_tag = text_node.parent
            if parent_tag and parent_tag.name not in ['pre', 'code', 'style', 'script']: # Keep whitespace in code/style/script
                normalized_text = re.sub(r'\s+', ' ', text_node.string)
                stripped_text = normalized_text.strip()
                if stripped_text:
                    text_node.replace_with(NavigableString(stripped_text))
                else:
                    text_node.extract() # Remove empty/whitespace-only nodes

    # Remove structurally empty elements (post-text normalization)
    # Repeat until no more changes, as removing one might empty its parent
    while True:
        removed_count = 0
        for tag in soup.find_all(True): # Iterate over all tags
            if tag.name not in ['br', 'hr'] and not tag.contents and not tag.attrs:
                 # Ensure it's truly empty, not just containing removed whitespace nodes
                 if not tag.get_text(strip=True):
                     logging.debug(f"Decomposing structurally empty tag: <{tag.name}>")
                     tag.decompose()
                     removed_count += 1
        if removed_count == 0:
            break


    # Merge adjacent paragraphs if they have identical attributes (or none)
    # Iterate backwards to handle multiple merges correctly
    paragraphs = soup.find_all('p')
    for i in range(len(paragraphs) - 1, 0, -1):
        current_p = paragraphs[i]
        prev_p = paragraphs[i-1]

        # Check if they are immediate siblings after text normalization
        if prev_p.next_sibling == current_p and current_p.attrs == prev_p.attrs:
            logging.debug(f"Merging adjacent paragraph into previous: {current_p.get_text()[:30]}...")
            # Append content with a space separator if both have text
            if prev_p.get_text(strip=True) and current_p.get_text(strip=True):
                 prev_p.append(NavigableString(" "))

            # Move children from current to previous
            for child in list(current_p.contents):
                prev_p.append(child)
            current_p.decompose()
            # Update the list in place might be tricky, re-finding might be safer if needed later
            # For this loop, just letting it continue is fine.

    return soup

def add_llm_metadata(soup: BeautifulSoup, original_url: str, original_title: str) -> BeautifulSoup:
    logging.debug("Adding LLM-specific metadata...")
    if not soup.body:
        logging.warning("Cannot add metadata, body tag not found.")
        return soup

    meta_section = soup.new_tag('section', attrs={"role": "complementary", "aria-label": "Document Metadata"})

    source_p = soup.new_tag('p')
    source_strong = soup.new_tag('strong')
    source_strong.string = "Source URL: "
    source_p.append(source_strong)
    source_a = soup.new_tag('a', href=original_url)
    source_a.string = original_url
    source_p.append(source_a)
    meta_section.append(source_p)

    date_p = soup.new_tag('p')
    date_strong = soup.new_tag('strong')
    date_strong.string = "Content Retrieved: "
    date_p.append(date_strong)
    date_p.append(NavigableString(datetime.now().strftime('%Y-%m-%d %H:%M:%S UTC')))
    meta_section.append(date_p)

    meta_section.append(soup.new_tag('hr'))

    # Insert metadata at the beginning of the body
    first_body_element = soup.body.find(True) # Find first actual element
    if first_body_element:
        first_body_element.insert_before(meta_section)
    else:
        soup.body.append(meta_section)

    # Ensure a H1 title exists if the original wasn't captured or isn't H1
    body_h1 = soup.body.find('h1')
    if not body_h1 and original_title:
        h1 = soup.new_tag('h1')
        h1.string = original_title
        meta_section.insert_after(h1) # Insert H1 after metadata
    elif body_h1 and original_title and body_h1.get_text(strip=True) != original_title:
        # If an H1 exists but doesn't match the page title, maybe add the title anyway?
        # Or trust the existing H1 is more relevant. Let's trust existing H1 for now.
        pass

    return soup


def clean_html_content(html_content: str, original_url: str) -> str | None:
    logging.info(f"Starting cleaning process for: {original_url}")
    try:
        # Use lxml parser for speed and robustness
        soup = BeautifulSoup(html_content, "lxml")

        original_title_tag = soup.find("title")
        original_title = original_title_tag.string.strip() if original_title_tag and original_title_tag.string else "Untitled Document"
        logging.debug(f"Original page title: '{original_title}'")

        # 1. Initial Strip of Undesirables
        soup = strip_undesirables(soup)

        # 2. Extract Main Content Area
        main_content_element = extract_main_content(soup)
        if not main_content_element:
             logging.error("Failed to extract any content element, returning None.")
             return None

        # 3. Create New Soup and Recursively Clean/Copy Allowed Content
        new_soup = BeautifulSoup("<!DOCTYPE html><html><head><meta charset='utf-8'><title></title></head><body></body></html>", "lxml")
        if soup.html and soup.html.get('lang'):
            new_soup.html['lang'] = soup.html['lang']
        else:
            new_soup.html['lang'] = 'en' # Default lang

        new_soup.title.string = f"Cleaned: {original_title}"

        logging.debug("Building cleaned structure...")
        if main_content_element.name == 'body': # If fallback was used
             source_elements = main_content_element.children
        else:
             source_elements = [main_content_element] # Process the container itself

        for element in source_elements:
            cleaned_element = clean_element_recursively(element, original_url)
            if cleaned_element:
                 # Handle unwrapped fragments
                 if isinstance(cleaned_element, Tag) and cleaned_element.name == 'div' and not cleaned_element.attrs:
                      for content in list(cleaned_element.contents):
                           new_soup.body.append(content)
                 else:
                      new_soup.body.append(cleaned_element)

        if not new_soup.body.contents:
             logging.warning(f"Body is empty after recursive cleaning for {original_url}. Check extraction/cleaning logic.")
             # Add a note indicating potential issues
             p_tag = new_soup.new_tag('p')
             em_tag = new_soup.new_tag('em')
             em_tag.string = "[URL Cleaner: No processable content found after filtering. The original page might have been dynamic, empty, or primarily non-textual.]"
             p_tag.append(em_tag)
             new_soup.body.append(p_tag)


        # 4. Post-Processing on the New Structure
        new_soup = simplify_tables(new_soup)
        new_soup = preserve_code_blocks(new_soup) # Ensure code formatting is okay
        new_soup = normalize_text_content(new_soup) # Normalize text and remove empty tags

        # 5. Add LLM Metadata
        new_soup = add_llm_metadata(new_soup, original_url, original_title)

        # 6. Final Output Formatting and Validation
        # Use 'minimal' or 'html' formatter. 'minimal' avoids extra whitespace.
        # Explicitly encode to utf-8 for consistency.
        cleaned_html = new_soup.prettify(formatter="minimal", encoding='utf-8').decode('utf-8')

        # Basic validation using lxml
        try:
            lxml_html.fromstring(cleaned_html.encode('utf-8'))
            logging.debug(f"Cleaned HTML for {original_url} passed basic lxml validation.")
        except (etree.XMLSyntaxError, etree.ParserError) as e:
            logging.warning(f"Cleaned HTML for {original_url} has structure issues (lxml validation): {e}")
            # Decide whether to return potentially broken HTML or None
            # For LLMs, slightly broken might be better than nothing, but log it clearly.
            pass # Return the HTML despite validation warning

        logging.info(f"Successfully cleaned content for: {original_url}")
        return cleaned_html

    except Exception as e:
        logging.exception(f"Critical error during HTML cleaning for {original_url}: {e}")
        return None


def save_cleaned_html(filename: str, html_content: str):
    filepath = OUTPUT_DIR / filename
    logging.info(f"Saving cleaned content to: {filepath.name}")
    try:
        with filepath.open("w", encoding="utf-8") as f:
            f.write(html_content)
    except IOError as e:
        logging.error(f"Failed to write output file {filepath}: {e}")
    except Exception as e:
        logging.error(f"Unexpected error saving file {filepath}: {e}")


def main():
    logging.info("--- Starting URL Content Cleaner ---")
    start_time = datetime.now()
    setup_environment()
    urls = load_urls(URL_LIST_FILE)

    if not urls:
        logging.warning(f"No URLs found in {URL_LIST_FILE.name}. Exiting.")
        sys.exit(0)

    headers = {"User-Agent": USER_AGENT, "Accept": "text/html,application/xhtml+xml;q=0.9,*/*;q=0.8"}
    # Increased limits slightly
    limits = httpx.Limits(max_keepalive_connections=30, max_connections=150)
    # Consider adding retry logic here if needed
    with httpx.Client(headers=headers, follow_redirects=True, timeout=REQUEST_TIMEOUT, limits=limits, verify=True) as client:
        processed_count = 0
        fetch_error_count = 0
        clean_error_count = 0
        skip_count = 0

        total_urls = len(urls)
        logging.info(f"Processing {total_urls} URLs...")

        for i, url in enumerate(urls, 1):
            logging.info(f"--- URL {i}/{total_urls}: {url} ---")
            try:
                parsed_url = urlparse(url)
                if parsed_url.scheme not in ('http', 'https'):
                    logging.warning(f"Skipping invalid URL scheme: {url}")
                    skip_count += 1
                    continue
            except ValueError:
                logging.warning(f"Skipping unparseable URL: {url}")
                skip_count += 1
                continue

            raw_html = fetch_url_content(url, client)
            if not raw_html:
                 # fetch_url_content logs the specific error
                 fetch_error_count += 1
                 continue # Move to next URL

            cleaned_html = clean_html_content(raw_html, url)
            if cleaned_html:
                output_filename = generate_filename(url)
                save_cleaned_html(output_filename, cleaned_html)
                processed_count += 1
            else:
                # clean_html_content logs the specific error
                logging.error(f"Failed to clean content for URL: {url}")
                clean_error_count += 1


    end_time = datetime.now()
    duration = end_time - start_time
    logging.info("--- Processing Summary ---")
    logging.info(f"Total URLs provided:        {total_urls}")
    logging.info(f"Successfully processed:     {processed_count}")
    logging.info(f"Skipped (invalid URL/type): {skip_count}")
    logging.info(f"Fetch errors:               {fetch_error_count}")
    logging.info(f"Cleaning errors:            {clean_error_count}")
    logging.info(f"Total execution time:       {duration}")
    logging.info("--- Finished ---")

    total_errors = fetch_error_count + clean_error_count
    if total_errors > 0:
        logging.warning("Processing finished with errors. Please review logs.")
        sys.exit(1)
    elif skip_count > 0 or processed_count < (total_urls - skip_count):
         logging.info("Processing finished. Some URLs were skipped or encountered non-critical issues (check logs).")
         sys.exit(0) # Consider 0 exit code if only skips occured
    else:
        logging.info("All processable URLs completed successfully.")
        sys.exit(0)

if __name__ == "__main__":
    main()
