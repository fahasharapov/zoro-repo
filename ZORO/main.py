"""Zoro Product Scraper

This script reads product names from an Excel spreadsheet (``test_items.xlsx``) and
searches Zoro.com for matching products. For each search term it captures up to the
first five product results, downloads their main product images, and saves the
aggregated data to ``zoro_results.xlsx``.

Usage
-----
1. Install dependencies::

       pip install pandas openpyxl requests beautifulsoup4 rapidfuzz playwright

   Optionally run ``playwright install`` if JavaScript rendering is required.

2. Place ``test_items.xlsx`` in the same directory as this script. The workbook
   must contain a column labelled ``Item Name``.

3. Run the script with Python 3.11 or newer::

       python zoro_scraper.py

The script will create a ``zoro_images`` folder (if needed) that stores downloaded
product images and an Excel file named ``zoro_results.xlsx`` containing the scraped
results.
"""
from __future__ import annotations

import random
import re
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, List, Optional

import pandas as pd
import requests
from bs4 import BeautifulSoup
from rapidfuzz import fuzz


SCRAPINGBEE_API_KEY = "64MS0CGV3U2L4FZF3SHCEYKE56LS6APVEOD9PQ6SW307XYMY11GNWC0NPMS6S0XFZFMQZ9G8PD7NFBCL"
SCRAPINGBEE_ENDPOINT = "https://app.scrapingbee.com/api/v1/"
USE_SCRAPINGBEE = True  # set to False if you want to revert to requests


# Optional dependency is imported lazily in ``fetch_html_with_playwright``.

# ---------------------------------------------------------------------------
# Configuration constants
# ---------------------------------------------------------------------------
BASE_URL = "https://www.zoro.com"
SEARCH_URL_TEMPLATE = BASE_URL + "/search?q={query}"
IMAGE_DIR_NAME = "zoro_images"
MAX_RESULTS_PER_ITEM = 5
FUZZY_MATCH_THRESHOLD = 50
REQUEST_TIMEOUT = 20
PLAYWRIGHT_USER_DATA_DIR = Path("zoro_profile")
PLAYWRIGHT_VIEWPORT = {"width": 1366, "height": 768}
PLAYWRIGHT_WAIT_SELECTORS = [
    "[data-test='productCard']",
    "[data-test='productCardTitle']",
]
PLAYWRIGHT_LAUNCH_ARGS = [
    "--disable-blink-features=AutomationControlled",
    "--disable-infobars",
    "--no-sandbox",
    "--disable-dev-shm-usage",
    "--disable-gpu",
]

DEBUG_MODE = False

DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": (
        "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif," 
        "image/webp,image/apng,*/*;q=0.8"
    ),
    "Accept-Encoding": "gzip, deflate, br",
    "Accept-Language": "en-US,en;q=0.9",
}


@dataclass
class ProductResult:
    """Container for storing product information."""

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
# Utility helpers
# ---------------------------------------------------------------------------

def slugify(value: str) -> str:
    """Convert a string into a filesystem-friendly slug."""

    value = value.strip().lower()
    value = re.sub(r"[^a-z0-9]+", "_", value)
    value = re.sub(r"_+", "_", value).strip("_")
    return value or "item"


def best_score(query: str, candidate: str) -> int:
    """Return the strongest fuzzy score between two strings."""

    if not query or not candidate:
        return 0

    return max(
        fuzz.token_set_ratio(query, candidate),
        fuzz.token_sort_ratio(query, candidate),
        fuzz.partial_ratio(query, candidate),
    )


# ---------------------------------------------------------------------------
# Excel ingestion
# ---------------------------------------------------------------------------

def read_excel_items(excel_path: Path) -> List[str]:
    """Read the Excel sheet and return a list of unique, non-empty item names."""

    if not excel_path.exists():
        raise FileNotFoundError(f"Input Excel file not found: {excel_path}")

    df = pd.read_excel(excel_path)
    if "Item Name" not in df.columns:
        raise KeyError("The Excel file must contain a column named 'Item Name'.")

    items: List[str] = []
    seen = set()
    for raw_value in df["Item Name"].astype(str).fillna(""):
        value = raw_value.strip()
        if not value or value.lower() == "nan":
            continue
        normalized = value.lower()
        if normalized in seen:
            continue
        seen.add(normalized)
        items.append(value)

    return items


# ---------------------------------------------------------------------------
# Networking helpers
# ---------------------------------------------------------------------------

_playwright_manager = None
_playwright_context = None
_playwright_page = None
_playwright_timeout_error = None


def _ensure_playwright_page():
    """Lazily initialise and return a persistent Playwright page."""

    global _playwright_manager, _playwright_context, _playwright_page, _playwright_timeout_error

    if _playwright_page is not None:
        return _playwright_page

    try:
        from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError
    except ImportError:
        return None

    try:
        PLAYWRIGHT_USER_DATA_DIR.mkdir(parents=True, exist_ok=True)
    except Exception:
        # Failure to create the directory should not abort scraping outright.
        pass

    try:
        _playwright_manager = sync_playwright().start()
        _playwright_context = _playwright_manager.chromium.launch_persistent_context(
            user_data_dir=str(PLAYWRIGHT_USER_DATA_DIR),
            headless=False,
            args=PLAYWRIGHT_LAUNCH_ARGS,
        )
        _playwright_context.set_default_timeout(REQUEST_TIMEOUT * 1000)

        _playwright_page = _playwright_context.new_page()
        _playwright_page.add_init_script(
            "Object.defineProperty(navigator, 'webdriver', {get: () => undefined})"
        )
        _playwright_page.set_user_agent(DEFAULT_HEADERS["User-Agent"])
        _playwright_page.set_viewport_size(PLAYWRIGHT_VIEWPORT)
        _playwright_page.set_extra_http_headers(
            {
                "Accept": DEFAULT_HEADERS.get("Accept", "*/*"),
                "Accept-Encoding": DEFAULT_HEADERS.get("Accept-Encoding", "gzip, deflate, br"),
                "Accept-Language": DEFAULT_HEADERS.get("Accept-Language", "en-US,en;q=0.9"),
                "Cache-Control": "no-cache",
                "Pragma": "no-cache",
                "Upgrade-Insecure-Requests": "1",
            }
        )

        _playwright_timeout_error = PlaywrightTimeoutError
        return _playwright_page
    except Exception:
        if _playwright_context is not None:
            try:
                _playwright_context.close()
            except Exception:
                pass
        if _playwright_manager is not None:
            try:
                _playwright_manager.stop()
            except Exception:
                pass
        _playwright_manager = None
        _playwright_context = None
        _playwright_page = None
        _playwright_timeout_error = None
        return None


def shutdown_playwright() -> None:
    """Close any persistent Playwright resources."""

    global _playwright_manager, _playwright_context, _playwright_page, _playwright_timeout_error

    if _playwright_page is not None:
        try:
            _playwright_page.close()
        except Exception:
            pass
    if _playwright_context is not None:
        try:
            _playwright_context.close()
        except Exception:
            pass
    if _playwright_manager is not None:
        try:
            _playwright_manager.stop()
        except Exception:
            pass

    _playwright_manager = None
    _playwright_context = None
    _playwright_page = None
    _playwright_timeout_error = None

def fetch_html_with_requests(url: str, session: requests.Session) -> Optional[str]:
    """Fetch HTML content using the requests library."""

    try:
        response = session.get(url, timeout=REQUEST_TIMEOUT)
        response.raise_for_status()
        return response.text
    except requests.RequestException:
        return None


def fetch_html_with_playwright(url: str) -> Optional[str]:
    """Fetch page HTML using a persistent Playwright browser context."""

    page = _ensure_playwright_page()
    if page is None or _playwright_timeout_error is None:
        return None

    html: Optional[str] = None
    max_attempts = 6
    cloudflare_logged = False

    for attempt in range(1, max_attempts + 1):
        try:
            if attempt == 1:
                page.goto(url, wait_until="domcontentloaded")
            else:
                page.reload(wait_until="domcontentloaded")
        except Exception:
            continue

        # Allow Cloudflare to complete by idling like a real user.
        wait_duration = random.uniform(8, 15)
        time.sleep(wait_duration)

        try:
            html = page.content()
        except Exception:
            html = None

        challenge_present = False
        if html:
            lowered = html.lower()
            if "data-cfasync" in lowered or "__cf_chl_jschl_tk__" in lowered:
                challenge_present = True

        if challenge_present:
            print(f"  * Cloudflare challenge detected, waiting… (attempt {attempt})")
            time.sleep(10)
            html = None
            continue

        product_card_visible = False
        product_title_visible = False

        try:
            page.wait_for_selector("[data-test='productCard']", state="visible", timeout=8000)
            product_card_visible = True
        except _playwright_timeout_error:
            try:
                page.wait_for_selector("[data-test='productCardTitle']", state="visible", timeout=5000)
                product_title_visible = True
            except _playwright_timeout_error:
                pass

        try:
            html = page.content()
        except Exception:
            pass

        html_contains_cards = False
        if html and "[data-test=\"productCard\"]" in html:
            html_contains_cards = True

        print(
            f"  * {page.url} - product cards detected: {'yes' if (product_card_visible or product_title_visible or html_contains_cards) else 'no'} (attempt {attempt})"
        )

        if (product_card_visible or html_contains_cards) and not cloudflare_logged:
            print("  * ✅ Passed Cloudflare challenge.")
            cloudflare_logged = True

        if product_card_visible or product_title_visible or html_contains_cards:
            break

        time.sleep(3)

    if DEBUG_MODE:
        preview = (html or "")[:500]
        print("  * Playwright HTML preview (first 500 chars):")
        print(preview)

    return html


# ---------------------------------------------------------------------------
# Parsing helpers
# ---------------------------------------------------------------------------

def parse_product_data(html: str, max_results: int = MAX_RESULTS_PER_ITEM) -> List[dict]:
    """Parse raw HTML and return structured product dictionaries.

    The function attempts to accommodate multiple markup structures that Zoro may
    use. It gathers the key product details needed for downstream processing.
    """

    soup = BeautifulSoup(html, "html.parser")

    card_source = "data-test-productCard"
    cards = soup.select("a[data-test='productCard']")
    if cards:
        print(f"  * Found {len(cards)} product cards via [data-test='productCard'] selector.")

    if not cards:
        seen_ids = set()
        fallback_cards: List = []
        for title_node in soup.select("[data-test='productCardTitle']"):
            anchor = title_node.find_parent("a")
            if anchor and id(anchor) not in seen_ids:
                seen_ids.add(id(anchor))
                fallback_cards.append(anchor)
        if fallback_cards:
            cards = fallback_cards
            card_source = "productCardTitle"
            print(
                f"  * Found {len(cards)} product cards via [data-test='productCardTitle'] fallback."
            )

    if not cards:
        cards = soup.select("a[href*='/i/']")
        if cards:
            card_source = "generic-anchor"
            print(
                f"  * Falling back to generic product anchors (found {len(cards)} matches)."
            )
    if not cards:
        print("  * No product cards detected after applying all selectors.")
        card_source = "none"

    results: List[dict] = []
    for card in cards:
        try:
            title_tag = card.select_one("[data-test='productCardTitle']") or card.select_one(
                "[data-test='product-title']"
            )
            title = ""
            if title_tag:
                title = title_tag.get_text(" ", strip=True)
            if not title:
                title = card.get("aria-label", "")
            if not title:
                title = card.get_text(" ", strip=True)
            if not title:
                nested_tag = card.select_one("div,span,h2,h3")
                if nested_tag:
                    title = nested_tag.get_text(" ", strip=True)

            href = card.get("href", "")
            url = href if href.startswith("http") else f"{BASE_URL}{href}" if href else ""

            price_tag = card.select_one("[data-test='productCardPrice']") or card.select_one(
                "[data-test='price']"
            )
            price = price_tag.get_text(strip=True) if price_tag else ""

            sku_tag = card.find(
                lambda tag: tag.name in {"span", "div"}
                and tag.get_text(strip=True).upper().startswith("SKU")
            )
            sku_text = sku_tag.get_text(strip=True) if sku_tag else ""
            sku = sku_text.replace("SKU", "").replace("#", "").strip()

            brand_tag = card.select_one("[data-test='product-brand']") or card.select_one(
                "[data-test='brand-name']"
            )
            brand = brand_tag.get_text(strip=True) if brand_tag else ""

            image_tag = card.select_one("img")
            image_url = ""
            if image_tag:
                image_url = (
                    image_tag.get("src")
                    or image_tag.get("data-src")
                    or image_tag.get("data-original")
                    or ""
                )

            if not title and not url:
                continue

            results.append(
                {
                    "title": title,
                    "url": url,
                    "price": price,
                    "sku": sku,
                    "brand": brand,
                    "image_url": image_url,
                    "source": card_source,
                }
            )
        except Exception:
            continue

        if len(results) >= max_results:
            break

    return results


# ---------------------------------------------------------------------------
# Core search logic
# ---------------------------------------------------------------------------

def search_zoro(item_name: str, session: requests.Session) -> List[ProductResult]:
    """Search Zoro for a given item and return the best-matching products."""

    query_url = SEARCH_URL_TEMPLATE.format(query=requests.utils.quote(item_name))

    html = fetch_html_with_playwright(query_url)
    if not html:
        print("  ! Playwright rendering failed; trying static HTML fetch.")
        html = fetch_html_with_requests(query_url, session)

    if not html:
        print(f"  ! Failed to retrieve results for '{item_name}'.")
        return []

    raw_results = parse_product_data(html, max_results=MAX_RESULTS_PER_ITEM * 2)
    if not raw_results:
        print("  ! No product cards detected on the page.")
        return []

    if DEBUG_MODE:
        generic_candidates = [raw for raw in raw_results if raw.get("source") == "generic-anchor"]
        if generic_candidates:
            debug_scores = []
            for raw in generic_candidates:
                title = raw.get("title", "")
                score = best_score(item_name, title)
                debug_scores.append((score, title, raw.get("url", "")))
            debug_scores.sort(key=lambda entry: entry[0], reverse=True)
            print("  * Debug: top generic anchor candidates")
            for idx, (score, title, url) in enumerate(debug_scores[:20], start=1):
                print(f"    {idx:02d}. score={score:>3} title={title[:120]} url={url}")

    results: List[ProductResult] = []
    for raw in raw_results:
        title = raw.get("title", "")
        url = raw.get("url", "")
        price = raw.get("price", "")
        sku = raw.get("sku", "")
        brand = raw.get("brand", "")
        image_url = raw.get("image_url", "")

        if not title and not url:
            continue

        match_score = best_score(item_name, title)
        if match_score < FUZZY_MATCH_THRESHOLD:
            continue

        results.append(
            ProductResult(
                search_term=item_name,
                title=title,
                url=url,
                price=price,
                sku=sku,
                brand=brand,
                image_url=image_url,
                image_path="",
                match_score=match_score,
            )
        )

        if len(results) >= MAX_RESULTS_PER_ITEM:
            break

    if not results:
        print("  ! No close matches met the fuzzy match threshold.")

    return results


# ---------------------------------------------------------------------------
# Image handling
# ---------------------------------------------------------------------------

def download_image(image_url: str, target_dir: Path, base_name: str, session: requests.Session) -> str:
    """Download an image if it does not already exist. Returns the saved path."""

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


# ---------------------------------------------------------------------------
# Output helpers
# ---------------------------------------------------------------------------

def save_to_excel(results: Iterable[ProductResult], output_path: Path) -> None:
    """Persist results to an Excel workbook."""

    rows = [
        {
            "Search Term": r.search_term,
            "Product Title": r.title,
            "Product URL": r.url,
            "Product Price": r.price,
            "SKU": r.sku,
            "Brand": r.brand,
            "Image URL": r.image_url,
            "Downloaded Image Path": r.image_path,
            "Match Score": r.match_score,
        }
        for r in results
    ]

    df = pd.DataFrame(rows)
    df.to_excel(output_path, index=False)


# ---------------------------------------------------------------------------
# Main execution
# ---------------------------------------------------------------------------

def main() -> None:
    """Primary execution routine for the script."""

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

    try:
        for item_name in items:
            print(f"Searching for: {item_name}...")
            results = search_zoro(item_name, session)

            if not results:
                all_results.append(
                    ProductResult(
                        search_term=item_name,
                        title="Not found",
                        url="",
                        price="",
                        sku="",
                        brand="",
                        image_url="",
                        image_path="",
                        match_score=0,
                    )
                )
                print(f"No results found for '{item_name}'.")
                consecutive_failures += 1
            else:
                consecutive_failures = 0
                for idx, product in enumerate(results, start=1):
                    base_name_parts = [slugify(item_name), str(idx)]
                    if product.sku:
                        base_name_parts.insert(1, slugify(product.sku))
                    base_name = "_".join(part for part in base_name_parts if part)

                    image_path = download_image(
                        product.image_url, image_dir, base_name, session
                    )
                    product.image_path = image_path
                    all_results.append(product)
                    print(f"  -> Found: {product.title} (Score: {product.match_score})")

                    if product.image_url:
                        time.sleep(random.uniform(2, 4))

            if consecutive_failures >= 3:
                print("  ! Encountered 3 consecutive failed searches. Backing off for 60 seconds.")
                time.sleep(60)
                consecutive_failures = 0

            # Random delay between requests to avoid hammering the server.
            delay = random.uniform(4, 9)
            time.sleep(delay)

        try:
            save_to_excel(all_results, output_path)
            print(f"Saved results to {output_path}")
        except Exception as exc:
            print(f"Failed to save results: {exc}")
    finally:
        shutdown_playwright()


if __name__ == "__main__":
    main()