import re
import json
# from dataclasses import dataclass
from urllib.parse import urlparse, urljoin, urldefrag

from bs4 import BeautifulSoup
from collections import Counter


# ----------------------------
# Configuration (declarative)
# ----------------------------

ALLOWED_DOMAIN_SUFFIXES = (
    ".ics.uci.edu",
    ".cs.uci.edu",
    ".informatics.uci.edu",
    ".stat.uci.edu",
)

# NOTE: assignment also allows today.uci.edu/department/information_computer_sciences/*
TODAY_HOST = "today.uci.edu"
TODAY_PREFIX = "/department/information_computer_sciences/"

IGNORED_EXTENSIONS = {
    # docs / archives
    "pdf", "doc", "docx", "ppt", "pptx", "xls", "xlsx", "csv", "rar", "zip",
    "gz", "tar", "tgz", "bz2", "7z", "iso",
    # media
    "jpg", "jpeg", "png", "gif", "tif", "tiff", "bmp", "webp", "ico",
    "mp3", "wav", "ogg", "mp4", "webm", "avi", "mov", "flv", "wmv",
    # scripts / misc non-html
    "css", "js", "xml", "json", "txt", "rss", "atom",
}

BLOCKED_QUERY_TOKENS = {
    "tribe-bar-date", "ical", "outlook-ical",
    "eventdisplay", "calendar", "date=",
    "sort=", "session", "replytocom", "share=",
}

BLOCKED_PATH_PATTERNS = [
    re.compile(r"/page/\d+", re.I),
    re.compile(r"/20\d{2}/\d{2}/\d{2}/", re.I),      # /YYYY/MM/DD/
    re.compile(r"/20\d{2}-\d{2}-\d{2}", re.I),       # /YYYY-MM-DD/
]

MAX_URL_LENGTH = 2000
MAX_PATH_DEPTH = 12
MAX_BYTES = 5_000_000  # 5 MB

STRIP_TAGS = ("script", "style", "noscript")

STOPWORDS = {
    "a","about","above","after","again","against","all","am","an","and","any","are","as","at","be",
    "because","been","before","being","below","between","both","but","by","could","did","do","does",
    "doing","down","during","each","few","for","from","further","had","has","have","having","he","her",
    "here","hers","herself","him","himself","his","how","i","if","in","into","is","it","its","itself",
    "just","me","more","most","my","myself","no","nor","not","now","of","off","on","once","only","or",
    "other","our","ours","ourselves","out","over","own","same","she","should","so","some","such","than",
    "that","the","their","theirs","them","themselves","then","there","these","they","this","those",
    "through","to","too","under","until","up","very","was","we","were","what","when","where","which",
    "while","who","whom","why","with","you","your","yours","yourself","yourselves"
}


# ----------------------------
# Analytics state
# ----------------------------

unique_urls = set()            # uniqueness by defragmented URL
word_counts = Counter()
subdomain_counts = Counter()   # only *.ics.uci.edu required by spec
longest_page = {"url": None, "word_count": 0}


# ----------------------------
# Helpers
# ----------------------------

def normalize_url(raw_url: str) -> str:
    """Defragment and strip whitespace. Leave query intact (spec only discards fragment)."""
    if not raw_url:
        return ""
    raw_url = raw_url.strip()
    clean, _ = urldefrag(raw_url)
    return clean

def looks_like_file(path: str) -> bool:
    path = (path or "").lower()
    # fast path: no dot in last segment
    last = path.rsplit("/", 1)[-1]
    if "." not in last:
        return False
    ext = last.rsplit(".", 1)[-1]
    return ext in IGNORED_EXTENSIONS

def response_is_html(resp) -> bool:
    """Return True if resp looks like an HTML response we should parse."""
    if resp is None or resp.raw_response is None:
        return False
    if resp.status != 200:
        return False

    headers = getattr(resp.raw_response, "headers", {}) or {}
    ctype = (headers.get("Content-Type") or "").lower()
    if "text/html" not in ctype:
        return False

    # respect Content-Length if present
    try:
        clen = int(headers.get("Content-Length", "0"))
        if clen and clen > MAX_BYTES:
            return False
    except ValueError:
        pass

    content = getattr(resp.raw_response, "content", None)
    if not content:
        return False
    if len(content) > MAX_BYTES:
        return False

    return True

def extract_visible_text(soup: BeautifulSoup) -> str:
    for tag in STRIP_TAGS:
        for node in soup.find_all(tag):
            node.decompose()
    return soup.get_text(separator=" ", strip=True)

def tokenize(text: str):
    # letters only; you can change this if you want to keep numbers
    tokens = re.findall(r"[A-Za-z]+", text.lower())
    return [t for t in tokens if t not in STOPWORDS]

def should_skip_low_value_page(text: str, soup: BeautifulSoup) -> bool:
    # minimal text
    if not text or len(text) < 50:
        return True
    # “link farm” heuristic: lots of links, almost no words
    words = re.findall(r"[A-Za-z]+", text)
    if len(words) < 100 and len(soup.find_all("a")) > 100:
        return True
    return False

def update_analytics(page_url: str, words):
    global longest_page

    if page_url in unique_urls:
        return

    unique_urls.add(page_url)
    word_counts.update(words)

    wc = len(words)
    if wc > longest_page["word_count"]:
        longest_page = {"url": page_url, "word_count": wc}

    # spec asks subdomains within ics.uci.edu
    host = (urlparse(page_url).hostname or "").lower()
    if host.endswith(".ics.uci.edu"):
        subdomain_counts[host] += 1

def save_progress():
    data = {
        "unique_pages": len(unique_urls),
        "longest_page": longest_page,
        "top_50_words": word_counts.most_common(50),
        "subdomains": dict(sorted(subdomain_counts.items())),
    }
    with open("crawler_stats.json", "w") as f:
        json.dump(data, f, indent=2)
    print(f"[save_progress] Saved {len(unique_urls)} pages so far.")


# ----------------------------
# Core crawler hooks
# ----------------------------

def scraper(url, resp):
    """Return list of valid URLs discovered on this page."""
    out_links = extract_next_links(url, resp)
    valid_links = [u for u in out_links if is_valid(u)]

    if len(unique_urls) > 0 and len(unique_urls) % 50 == 0:
        save_progress()

    return valid_links

def extract_next_links(url, resp):
    if not response_is_html(resp):
        return []

    url = normalize_url(url)
    parsed = urlparse(url)
    if looks_like_file(parsed.path):
        return []

    # Parse HTML
    soup = BeautifulSoup(resp.raw_response.content, "html.parser")
    text = extract_visible_text(soup)

    if should_skip_low_value_page(text, soup):
        return []

    words = tokenize(text)
    update_analytics(url, words)

    next_links = []
    for a in soup.find_all("a", href=True):
        href = a.get("href", "").strip()
        if not href:
            continue
        # skip obvious non-web links early
        if href.startswith(("mailto:", "tel:", "javascript:")):
            continue

        abs_url = urljoin(url, href)
        abs_url = normalize_url(abs_url)
        if abs_url:
            next_links.append(abs_url)

    return next_links

def is_valid(url):
    try:
        url = normalize_url(url)
        parsed = urlparse(url)

        if parsed.scheme not in {"http", "https"}:
            return False

        if len(url) > MAX_URL_LENGTH:
            return False

        # avoid super deep paths (trap-ish)
        if url.count("/") > MAX_PATH_DEPTH:
            return False

        host = (parsed.netloc or "").lower()
        path = (parsed.path or "").lower()
        query = (parsed.query or "").lower()

        # today.uci.edu restriction (required by spec)
        if host == TODAY_HOST:
            if not path.startswith(TODAY_PREFIX):
                return False
        else:
            if not any(host.endswith(suf) for suf in ALLOWED_DOMAIN_SUFFIXES):
                return False

        if looks_like_file(path):
            return False

        if any(tok in query for tok in BLOCKED_QUERY_TOKENS):
            return False

        for rx in BLOCKED_PATH_PATTERNS:
            if rx.search(path):
                return False

        # a few WordPress-ish low value families
        if any(seg in path for seg in ("/category/", "/author/", "/tag/", "/feed/")):
            return False

        return True

    except Exception as e:
        print(f"[is_valid] Error validating {url}: {e}")
        return False


def verify_crawl():
    save_progress()
    print(f"Total unique pages: {len(unique_urls)}")
    print(f"Longest page: {longest_page['url']} ({longest_page['word_count']} words)")
    print("Top 10 words:")
    for w, c in word_counts.most_common(10):
        print(f"{w}: {c}")
