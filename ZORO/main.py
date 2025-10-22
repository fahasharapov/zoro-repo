"""Zoro Product Scraper — ScrapingBee + Playwright + Requests Hybrid

Now includes:
✅ ScrapingBee for Cloudflare-safe scraping
✅ Playwright fallback if ScrapingBee fails
✅ Requests backup for static content
✅ Smarter fuzzy matching, normalization, and /i/ link fallbacks
"""

from __future__ import annotations
import random, re, time
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, List, Optional

import pandas as pd
import requests
from bs4 import BeautifulSoup
from rapidfuzz import fuzz

# ---------------------------------------------------------------------------
# ScrapingBee configuration
# ---------------------------------------------------------------------------
SCRAPINGBEE_API_KEY = "64MS0CGV3U2L4FZF3SHCEYKE56LS6APVEOD9PQ6SW307XYMY11GNWC0NPMS6S0XFZFMQZ9G8PD7NFBCL"
SCRAPINGBEE_ENDPOINT = "https://app.scrapingbee.com/api/v1/"
USE_SCRAPINGBEE = True

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
BASE_URL = "https://www.zoro.com"
SEARCH_URL_TEMPLATE = BASE_URL + "/search?q={query}"
IMAGE_DIR_NAME = "zoro_images"
MAX_RESULTS_PER_ITEM = 5
FUZZY_MATCH_THRESHOLD = 35
REQUEST_TIMEOUT = 25
DEBUG_MODE = False

DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "en-US,en;q=0.9",
}

# ---------------------------------------------------------------------------
# Dataclass for results
# ---------------------------------------------------------------------------
@dataclass
class ProductResult:
    search_term: str
    title: str
    url: str
    price: str
    sku: str
    brand: str
    image_url: str
    image_path: str
    match_score: int

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------
def slugify(value: str) -> str:
    value = value.strip().lower()
    value = re.sub(r"[^a-z0-9]+", "_", value)
    value = re.sub(r"_+", "_", value).strip("_")
    return value or "item"

def normalize_text(s: str) -> str:
    s = s.lower()
    s = re.sub(r"[^a-z0-9]+", " ", s)
    return s.strip()

def best_score(query: str, candidate: str) -> int:
    if not query or not candidate:
        return 0
    q, c = normalize_text(query), normalize_text(candidate)
    return max(
        fuzz.token_set_ratio(q, c),
        fuzz.token_sort_ratio(q, c),
        fuzz.partial_ratio(q, c),
    )

# ---------------------------------------------------------------------------
# Excel ingestion
# ---------------------------------------------------------------------------
def read_excel_items(excel_path: Path) -> List[str]:
    if not excel_path.exists():
        raise FileNotFoundError(f"Input Excel file not found: {excel_path}")
    df = pd.read_excel(excel_path)
    if "Item Name" not in df.columns:
        raise KeyError("The Excel file must contain a column named 'Item Name'.")
    items, seen = [], set()
    for raw_value in df["Item Name"].astype(str).fillna(""):
        value = raw_value.strip()
        if not value or value.lower() == "nan":
            continue
        norm = value.lower()
        if norm not in seen:
            seen.add(norm)
            items.append(value)
    return items

# ---------------------------------------------------------------------------
# Networking helpers
# ---------------------------------------------------------------------------
def fetch_html_with_scrapingbee(url: str) -> Optional[str]:
    try:
        params = {
            "api_key": SCRAPINGBEE_API_KEY,
            "url": url,
            "render_js": "true",
            "wait": "4000",
            "block_ads": "true",
        }
        print(f"  * ScrapingBee fetching: {url}")
        response = requests.get(SCRAPINGBEE_ENDPOINT, params=params, timeout=REQUEST_TIMEOUT)
        if response.status_code == 200:
            return response.text
        print(f"  ! ScrapingBee error {response.status_code}: {response.text[:120]}")
        return None
    except Exception as exc:
        print(f"  ! ScrapingBee request failed: {exc}")
        return None

def fetch_html_with_playwright(url: str) -> Optional[str]:
    try:
        from playwright.sync_api import sync_playwright
    except ImportError:
        print("  ! Playwright not installed. Skipping.")
        return None

    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True, args=["--no-sandbox"])
            context = browser.new_context()
            page = context.new_page()
            page.set_default_timeout(REQUEST_TIMEOUT * 1000)
            page.goto(url, wait_until="domcontentloaded")
            time.sleep(random.uniform(6, 10))
            html = page.content()
            browser.close()
            return html
    except Exception as exc:
        print(f"  ! Playwright error: {exc}")
        return None

def fetch_html_with_requests(url: str, session: requests.Session) -> Optional[str]:
    try:
        response = session.get(url, timeout=REQUEST_TIMEOUT)
        response.raise_for_status()
        return response.text
    except requests.RequestException:
        return None

# ---------------------------------------------------------------------------
# Parsing helpers
# ---------------------------------------------------------------------------
def parse_product_data(html: str, max_results: int = MAX_RESULTS_PER_ITEM) -> List[dict]:
    soup = BeautifulSoup(html, "html.parser")
    cards = soup.select("a[data-test='productCard']")
    if cards:
        print(f"  * Found {len(cards)} product cards via [data-test='productCard'].")
    if not cards:
        fallback = soup.select("[data-test='productCardTitle']")
        if fallback:
            print(f"  * Found {len(fallback)} via [data-test='productCardTitle'] fallback.")
            cards = fallback
    if not cards:
        cards = soup.select("a[href*='/i/']")
        if cards:
            print(f"  * Fallback: found {len(cards)} generic anchors.")
    if not cards:
        for tag in soup.find_all("a", href=True):
            if "/i/" in tag["href"]:
                cards.append(tag)
        if cards:
            print(f"  * Ultimate fallback found {len(cards)} /i/ links.")
    if not cards:
        print("  * No product cards detected after all selectors.")

    results: List[dict] = []
    for card in cards:
        try:
            title_tag = card.select_one("[data-test='productCardTitle']") or card.select_one("h2,h3,span,div")
            title = title_tag.get_text(" ", strip=True) if title_tag else card.get_text(" ", strip=True)
            href = card.get("href", "")
            url = href if href.startswith("http") else f"{BASE_URL}{href}" if href else ""
            price_tag = card.select_one("[data-test='productCardPrice']") or card.select_one("[data-test='price']")
            price = price_tag.get_text(strip=True) if price_tag else ""
            brand_tag = card.select_one("[data-test='product-brand']") or card.select_one("[data-test='brand-name']")
            brand = brand_tag.get_text(strip=True) if brand_tag else ""
            image_tag = card.select_one("img")
            image_url = image_tag.get("src") or image_tag.get("data-src") or "" if image_tag else ""
            if not title and not url:
                continue
            results.append({
                "title": title,
                "url": url,
                "price": price,
                "brand": brand,
                "image_url": image_url,
            })
        except Exception:
            continue
        if len(results) >= max_results:
            break
    return results

# ---------------------------------------------------------------------------
# Core search logic with triple fallback
# ---------------------------------------------------------------------------
def search_zoro(item_name: str, session: requests.Session) -> List[ProductResult]:
    query_url = SEARCH_URL_TEMPLATE.format(query=requests.utils.quote(item_name))
    html = None

    # 1️⃣ Try ScrapingBee first
    if USE_SCRAPINGBEE:
        html = fetch_html_with_scrapingbee(query_url)

    # 2️⃣ Fallback to Playwright
    if not html:
        print("  ! ScrapingBee failed — trying Playwright...")
        html = fetch_html_with_playwright(query_url)

    # 3️⃣ Final fallback to requests
    if not html:
        print("  ! Playwright failed — trying plain requests.")
        html = fetch_html_with_requests(query_url, session)

    if not html:
        print(f"  ❌ Failed to retrieve results for '{item_name}'.")
        return []

    raw_results = parse_product_data(html, max_results=MAX_RESULTS_PER_ITEM * 2)
    if not raw_results:
        print("  ! No product cards detected on the page.")
        return []

    results: List[ProductResult] = []
    for raw in raw_results:
        title = raw.get("title", "")
        url = raw.get("url", "")
        price = raw.get("price", "")
        brand = raw.get("brand", "")
        image_url = raw.get("image_url", "")
        match_score = best_score(item_name, title)
        if match_score < FUZZY_MATCH_THRESHOLD:
            continue
        results.append(ProductResult(
            search_term=item_name,
            title=title,
            url=url,
            price=price,
            sku="",
            brand=brand,
            image_url=image_url,
            image_path="",
            match_score=match_score,
        ))
        if len(results) >= MAX_RESULTS_PER_ITEM:
            break

    if not results:
        print("  ! No close matches met the fuzzy match threshold.")
    return results

# ---------------------------------------------------------------------------
# Image and Excel I/O
# ---------------------------------------------------------------------------
def download_image(image_url: str, target_dir: Path, base_name: str, session: requests.Session) -> str:
    if not image_url:
        return ""
    target_dir.mkdir(parents=True, exist_ok=True)
    file_path = target_dir / f"{base_name}.jpg"
    if file_path.exists():
        return str(file_path)
    try:
        response = session.get(image_url, stream=True, timeout=REQUEST_TIMEOUT)
        response.raise_for_status()
        with open(file_path, "wb") as fh:
            for chunk in response.iter_content(chunk_size=8192):
                fh.write(chunk)
        return str(file_path)
    except requests.RequestException:
        if file_path.exists():
            file_path.unlink(missing_ok=True)
        return ""

def save_to_excel(results: Iterable[ProductResult], output_path: Path) -> None:
    rows = [{
        "Search Term": r.search_term,
        "Product Title": r.title,
        "Product URL": r.url,
        "Product Price": r.price,
        "Brand": r.brand,
        "Image URL": r.image_url,
        "Downloaded Image Path": r.image_path,
        "Match Score": r.match_score,
    } for r in results]
    pd.DataFrame(rows).to_excel(output_path, index=False)

# ---------------------------------------------------------------------------
# Main execution
# ---------------------------------------------------------------------------
def main() -> None:
    base_dir = Path(__file__).resolve().parent
    excel_path = base_dir / "test_items.xlsx"
    output_path = base_dir / "zoro_results.xlsx"
    image_dir = base_dir / IMAGE_DIR_NAME

    try:
        items = read_excel_items(excel_path)
    except Exception as exc:
        print(f"Failed to read Excel file: {exc}")
        return
    if not items:
        print("No valid items found in the Excel file.")
        return

    session = requests.Session()
    session.headers.update(DEFAULT_HEADERS)
    all_results: List[ProductResult] = []
    consecutive_failures = 0

    for item_name in items:
        print(f"\n🔎 Searching for: {item_name}...")
        results = search_zoro(item_name, session)

        if not results:
            all_results.append(ProductResult(
                search_term=item_name,
                title="Not found",
                url="",
                price="",
                sku="",
                brand="",
                image_url="",
                image_path="",
                match_score=0,
            ))
            consecutive_failures += 1
        else:
            consecutive_failures = 0
            for idx, product in enumerate(results, start=1):
                base_name = "_".join(filter(None, [slugify(item_name), str(idx)]))
                product.image_path = download_image(product.image_url, image_dir, base_name, session)
                all_results.append(product)
                print(f"  ✅ {product.title} ({product.match_score})")

                if product.image_url:
                    time.sleep(random.uniform(2, 4))

        if consecutive_failures >= 3:
            print("⚠️  Encountered 3 failed searches. Pausing for 60 seconds...")
            time.sleep(60)
            consecutive_failures = 0

        time.sleep(random.uniform(4, 9))

    try:
        save_to_excel(all_results, output_path)
        print(f"\n💾 Saved results to {output_path}")
    except Exception as exc:
        print(f"Failed to save results: {exc}")

if __name__ == "__main__":
    main()
