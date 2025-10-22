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

# Optional dependency is imported lazily in ``fetch_html_with_playwright``.

# ---------------------------------------------------------------------------
# Configuration constants
# ---------------------------------------------------------------------------
BASE_URL = "https://www.zoro.com"
SEARCH_URL_TEMPLATE = BASE_URL + "/search?q={query}"
IMAGE_DIR_NAME = "zoro_images"
MAX_RESULTS_PER_ITEM = 5
FUZZY_MATCH_THRESHOLD = 60
REQUEST_TIMEOUT = 20
PLAYWRIGHT_WAIT_SELECTORS = [
    "[data-testid='plp-product-card']",
    "[data-testid='plp-product-card-container']",
    "article[data-testid='product-card']",
    "div[data-testid='product-card']",
    "a[data-testid='product-title']",
]

DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
    ),
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

def fetch_html_with_requests(url: str, session: requests.Session) -> Optional[str]:
    """Fetch HTML content using the requests library."""

    try:
        response = session.get(url, timeout=REQUEST_TIMEOUT)
        response.raise_for_status()
        return response.text
    except requests.RequestException:
        return None


def fetch_html_with_playwright(url: str) -> Optional[str]:
    """Fetch page HTML using Playwright in headless mode.

    Returns ``None`` if Playwright is unavailable or the page could not be
    rendered successfully.
    """

    try:
        from playwright.sync_api import sync_playwright
    except ImportError:
        return None

    try:
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=True)
            page = browser.new_page()
            page.goto(url, wait_until="domcontentloaded", timeout=REQUEST_TIMEOUT * 1000)

            for selector in PLAYWRIGHT_WAIT_SELECTORS:
                try:
                    page.wait_for_selector(selector, timeout=5000, state="visible")
                    break
                except Exception:
                    continue
            else:
                # If no selector matched, allow some time for dynamic content.
                page.wait_for_timeout(2000)

            html = page.content()
            browser.close()
            return html
    except Exception:
        return None


# ---------------------------------------------------------------------------
# Parsing helpers
# ---------------------------------------------------------------------------

def parse_product_data(html: str, max_results: int = MAX_RESULTS_PER_ITEM) -> List[dict]:
    """Parse raw HTML and return structured product dictionaries.

    The function attempts to accommodate multiple markup structures that Zoro may
    use. It gathers the key product details needed for downstream processing.
    """

    soup = BeautifulSoup(html, "html.parser")

    selectors = [
        "[data-testid='plp-product-card']",
        "[data-testid='plp-product-card-container']",
        "article[data-testid='product-card']",
        "div[data-testid='product-card']",
        "li[class*='ProductCard']",
    ]

    cards: List = []
    for selector in selectors:
        matches = soup.select(selector)
        if matches:
            cards = matches
            break

    if not cards:
        # Fall back to a more generic search for anchor tags representing products.
        cards = soup.select("a[href*='/i/']")

    results: List[dict] = []
    for card in cards:
        try:
            title_tag = card.select_one(
                "a[data-testid='product-title'], a[data-za-detail='ProductName'], "
                "a[href*='/i/'], h2, h3"
            )
            title = title_tag.get_text(strip=True) if title_tag else ""

            link_tag = card.select_one("a[data-testid='product-title']") or card.select_one(
                "a[href*='/i/']"
            )
            url = ""
            if link_tag:
                href = link_tag.get("href", "")
                url = href if href.startswith("http") else BASE_URL + href

            price_tag = (
                card.select_one("[data-testid='price'] span")
                or card.select_one("[data-testid='price']")
                or card.select_one("[data-testid='product-price']")
                or card.select_one("div[class*='Price'] span")
                or card.select_one("span[class*='price']")
            )
            price = price_tag.get_text(strip=True) if price_tag else ""

            sku_tag = card.find(lambda tag: tag.name in {"span", "div"} and "SKU" in tag.get_text())
            sku_text = sku_tag.get_text(strip=True) if sku_tag else ""
            sku = sku_text.replace("SKU", "").replace("#", "").strip()

            brand_tag = card.select_one("[data-testid='brand-name']") or card.select_one(
                "[data-testid='product-brand']"
            )
            brand = brand_tag.get_text(strip=True) if brand_tag else ""

            image_tag = card.select_one("img[data-testid='product-image']") or card.select_one("img")
            image_url = ""
            if image_tag:
                image_url = image_tag.get("src") or image_tag.get("data-src") or ""

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

        match_score = fuzz.token_set_ratio(item_name, title)
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
        else:
            for idx, product in enumerate(results, start=1):
                base_name_parts = [slugify(item_name), str(idx)]
                if product.sku:
                    base_name_parts.insert(1, slugify(product.sku))
                base_name = "_".join(part for part in base_name_parts if part)

                image_path = download_image(product.image_url, image_dir, base_name, session)
                product.image_path = image_path
                all_results.append(product)
                print(f"  -> Found: {product.title} (Score: {product.match_score})")

        # Random delay between requests to avoid hammering the server.
        delay = random.uniform(1, 3)
        time.sleep(delay)

    try:
        save_to_excel(all_results, output_path)
        print(f"Saved results to {output_path}")
    except Exception as exc:
        print(f"Failed to save results: {exc}")


if __name__ == "__main__":
    main()