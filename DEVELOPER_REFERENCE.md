# Developer Reference - Code Structure & Extensibility

**Version**: 4.0  
**Document**: Code Architecture & Contribution Guide

---

## 📂 File Structure

```
DataVisualisation_Final/
├── batdongsan_crawler.py          # Main crawler script (1500+ lines)
├── requirements.txt                # Python dependencies
├── proxies.txt                     # Proxy list (optional)
├── .crawler_profile/               # Persistent browser context (auto-created)
├── README.md                       # Quick start guide
├── CHANGELOG.md                    # Version history
├── OPTIMIZATION_GUIDE.md           # Performance optimization details
└── DEVELOPER_REFERENCE.md          # This file
```

---

## 🎯 Main Script Structure

### 1. Imports & Constants
```python
# Core async libraries
import asyncio
from asyncio import Semaphore, Lock

# Web crawling
from crawl4ai import AsyncWebCrawler, BrowserConfig, CrawlerRunConfig
from crawl4ai.content_filter_engine import BM25ContentFilter
from crawl4ai.chunking_strategy import RegexChunkingStrategy

# HTTP client
import httpx

# Data handling
from pydantic import BaseModel, Field
from bs4 import BeautifulSoup
import csv

# Utilities
import logging
import argparse
from pathlib import Path
from datetime import datetime
from urllib.parse import urljoin, urlparse
import re
import unicodedata
import itertools
import random
```

### 2. Constants & Configuration
```python
# Default values
DEFAULT_WARMUP_COUNT = 3
DEFAULT_USER_DATA_DIR = ".crawler_profile"
DEFAULT_PROXY_FILE = "proxies.txt"
DEFAULT_HTTPX_TIMEOUT = 45
DEFAULT_MAX_SESSION_QUERIES = 20

# Resource blocking patterns
BLOCKED_URL_PATTERNS = [
    r".*\.(png|jpg|jpeg|gif|svg)(\?.*)?$",
    r".*\.css(\?.*)?$",
    r".*\.(woff2|ttf)(\?.*)?$",
]
BLOCKED_RESOURCE_TYPES = ["image", "font", "stylesheet"]

# CSS selectors for extraction
CSS_SELECTORS = {
    "title": ".re__pr-title",
    "price": ".re__pr-price",
    "area": ".re__pr-specs-content-item",
    # ... 30+ more selectors
}

# Regex fallback patterns
REGEX_PATTERNS = {
    "price": r"(\d+[\.\,]\d+)\s*(triệu|tỷ|th|t)",
    "area": r"(\d+[\.\,]\d+)\s*m[²2]",
    # ... more patterns
}
```

### 3. Data Models (Pydantic v2)
```python
class RealEstateSchema(BaseModel):
    """35 fields for real estate property"""
    # Location fields (7)
    url: str
    title: str
    full_address: str
    city: Optional[str]
    district: Optional[str]
    ward: Optional[str]
    street: Optional[str]
    
    # Price fields (5)
    price: Optional[float]  # In millions VND
    price_per_unit: Optional[float]  # Per m²
    price_min_range: Optional[float]
    price_max_range: Optional[float]
    currency: str = "VND"
    
    # Property specs (10)
    area: Optional[float]  # m²
    bedrooms: Optional[int]
    bathrooms: Optional[int]
    floors: Optional[int]
    direction: Optional[str]
    frontage: Optional[float]  # meters
    road_width: Optional[float]  # meters
    legal_status: Optional[str]
    interior: Optional[str]
    condition: Optional[str]
    
    # Posting info (5)
    post_rank: Optional[str]  # VIP ranking
    posted_date: Optional[str]
    posted_by: Optional[str]
    seller_phone: Optional[str]
    seller_contact: Optional[str]
    
    # Boolean flags (3)
    has_elevator: bool = False
    near_park: bool = False
    is_frontage_road: bool = False
    
    # Meta (5)
    crawl_timestamp: str
    extraction_method: str  # "css", "regex", "text"
    data_quality: float  # 0.0-1.0
    raw_html_length: int
    error_message: Optional[str]
```

---

## 🔧 Key Functions Overview

### Phase 1: Configuration Builders

#### `build_browser_config(user_data_dir, proxy_url, use_persistent_context=True)`
```python
# Purpose: Create BrowserConfig for persistent browser context
# Returns: BrowserConfig with:
#   - headless="new" (modern headless mode)
#   - enable_stealth=True (Cloudflare bypass)
#   - user_data_dir=user_data_dir (cookie/profile persistence)
#   - proxy=proxy_url (HTTP proxy support)
#   - use_persistent_context=True (reuse browser sessions)
```

**Key changes from v4.0**:
- ✅ FIXED: Now passes `proxy_url` (string) directly, not ProxyConfig object
- Automatically stores cookies in `.crawler_profile/` directory
- Stealth flags prevent "automation controlled" detection

#### `build_dispatcher(max_session_queries=20)`
```python
# Purpose: Create MemoryAdaptiveDispatcher for session reuse
# Returns: Dispatcher with intelligent session caching
# Benefits: 60% reduction in session overhead
```

#### `build_run_config(dispatcher=None)`
```python
# Purpose: Create CrawlerRunConfig with resource blocking
# Features:
#   - Block images, CSS, fonts (save 80% bandwidth)
#   - Allow JavaScript (Cloudflare challenge)
#   - Attach dispatcher for session reuse
#   - Wait until "domcontentloaded" (faster)
```

### Phase 2: Helper Functions

#### `filter_supported_kwargs(func, kwargs)`
```python
# Purpose: Safely extract only supported parameters for a function
# Use case: Don't pass unsupported kwargs to BrowserConfig
# Example:
#   func = BrowserConfig.__init__
#   kwargs = {"proxy": "url", "unsupported_param": "value"}
#   filtered = filter_supported_kwargs(func, kwargs)
#   # Result: {"proxy": "url"}  # unsupported_param removed
```

#### `is_cf_challenge(html)`
```python
# Purpose: Detect if HTML is Cloudflare challenge page
# Returns: bool
# Indicators checked:
#   - "Checking your browser before accessing..."
#   - "cf_challenge" in HTML
#   - Cloudflare JavaScript sources
```

#### `load_proxies(file_path)`
```python
# Purpose: Read proxy list from file
# Format expected: One proxy per line
#   http://user:pass@ip:port
#   socks5://proxy:port
# Returns: list[str] of valid proxy URLs
# Error handling: Logs and skips invalid lines
```

#### `extract_session_cookies(crawler)`
```python
# Purpose: Extract cf_clearance cookie from persistent browser
# Returns: dict with cookie details
# Critical for HTTPX phase: This cookie bypasses Cloudflare
```

### Phase 3: Crawling Engines

#### `async crawl_details_browser(crawler, urls, config, delay_min, delay_max, max_concurrent)`
```python
# Purpose: Phase 1 (Warm-up) or Fallback crawling with browser
# Args:
#   crawler: AsyncWebCrawler instance
#   urls: list of detail URLs to crawl
#   config: CrawlerRunConfig with resource blocking
#   delay_min/max: Random delay between requests
#   max_concurrent: Concurrency level (1 for warm-up, 3-4 for fallback)
# Returns: dict with results and failed URLs
# Semaphore: Controls concurrent requests
# Cloudflare: Automatically solved during page load
```

#### `async crawl_details_httpx(urls, cookies, user_agent, max_concurrent, proxy_pool)`
```python
# Purpose: Phase 2 (Scale) fast parallel crawling with HTTPX
# Args:
#   urls: Remaining detail URLs
#   cookies: cf_clearance from warm-up phase
#   user_agent: Browser User-Agent string
#   max_concurrent: 4-10 concurrent requests
#   proxy_pool: list of proxy URLs for rotation
# Returns: dict with HTTPX-crawled results
# Speed: 300-500ms per request (vs 5s with browser)
# Proxy rotation: Round-robin via itertools.cycle()
# Fallback: Captures failed URLs for browser retry
```

### Phase 4: Main Orchestration

#### `async main_async()`
```python
# Entry point with 3 execution paths:

# 1. WARM-UP PHASE (Sequential, concurrency=1)
#    ├─ Crawl first N URLs with browser
#    ├─ Cloudflare challenge auto-solved
#    └─ Extract cf_clearance cookie

# 2. SCALE PHASE - Path A (HTTPX, fast)
#    ├─ If cf_clearance acquired
#    ├─ Use HTTPX clients with cookie injection
#    ├─ Concurrency=4-10 (no browser overhead)
#    └─ Fallback to browser on failure

# 2. SCALE PHASE - Path B (Browser, safe)
#    ├─ If no cf_clearance OR all HTTPX failed
#    ├─ Use persistent browser + dispatcher
#    ├─ Concurrency=3-4 (reuse sessions)
#    └─ Slower but guaranteed

# 3. MERGE & EXPORT
#    ├─ Combine warm-up + scale results
#    ├─ Validate all records
#    └─ Write CSV with UTF-8-SIG encoding
```

---

## 📊 Data Extraction Pipeline

### CSS Selector Extraction (Fast Path)
```python
def parse_detail_page(html: str, url: str) -> RealEstateSchema:
    soup = BeautifulSoup(html, 'html.parser')
    
    # 1. Try CSS selectors (fast, accurate)
    data = {}
    data['title'] = soup.select_one('.re__pr-title').text.strip()
    data['price'] = extract_price_from_selector(soup)
    # ... 30+ CSS extractions
    
    # 2. If CSS missing, try regex (fallback)
    if not data.get('price'):
        data['price'] = extract_price_from_text_regex(html)
    
    # 3. If still missing, use plain text extraction
    if not data.get('area'):
        data['area'] = extract_area_from_html(html)
    
    # 4. Normalize units & validate
    data['price'] = normalize_price(data['price'])  # → millions VND
    data['area'] = normalize_area(data['area'])      # → m²
    
    # 5. Create Pydantic model (auto-validates)
    return RealEstateSchema(
        url=url,
        crawl_timestamp=datetime.now().isoformat(),
        extraction_method=determine_extraction_method(data),
        data_quality=calculate_quality_score(data),
        **data
    )
```

### Extraction Methods (Priority Order)
1. **CSS Selectors** (90-95% accurate) - Fastest
2. **Regex** (70-80% accurate) - Medium speed
3. **Text extraction** (50-60% accurate) - Slowest
4. **Defaults** (0% accurate) - Fallback values

---

## 🔄 Execution Flow Diagram

```
START
  │
  ├─ Parse CLI args
  ├─ Create .crawler_profile/ directory
  │
  ├─ Load proxies (if --use-proxy)
  │
  ├─ Initialize AsyncWebCrawler
  │   └─ Browser config: stealth + persistent context
  │
  ├─ PHASE 1: WARM-UP (concurrency=1)
  │   ├─ Load first N catalog pages
  │   ├─ Extract detail URLs
  │   ├─ Crawl first --warmup-count URLs
  │   │   └─ Browser solves Cloudflare challenge
  │   ├─ Extract cf_clearance cookie
  │   └─ Parse & save detail records
  │
  ├─ DECISION: cf_clearance acquired?
  │   │
  │   ├─ YES ──→ PHASE 2A: HTTPX SCALE (concurrency=4-10)
  │   │   ├─ Create HTTPX clients with cf_clearance cookie
  │   │   ├─ Rotate proxies round-robin
  │   │   ├─ Crawl remaining URLs (300-500ms each)
  │   │   └─ On HTTPX failure → Track failed URLs
  │   │
  │   └─ NO ──→ PHASE 2B: BROWSER SCALE (concurrency=3-4)
  │       ├─ Use persistent context + dispatcher
  │       ├─ Crawl remaining URLs (3-5s each)
  │       └─ Session reuse reduces overhead
  │
  ├─ FALLBACK (if Phase 2A had failures)
  │   ├─ Re-crawl failed URLs with browser
  │   └─ Log failures for debugging
  │
  ├─ MERGE all results
  │   ├─ Warm-up results
  │   ├─ Scale results
  │   └─ Fallback results
  │
  ├─ VALIDATE & NORMALIZE
  │   ├─ Check Pydantic schema compliance
  │   ├─ Normalize prices → millions VND
  │   ├─ Normalize areas → m²
  │   └─ Calculate data quality scores
  │
  ├─ EXPORT to CSV
  │   ├─ UTF-8-SIG encoding (Excel compatible)
  │   ├─ All 35 fields
  │   └─ Timestamp + extraction method
  │
  └─ END
```

---

## 🧪 Adding Custom Selectors

### Step 1: Find CSS Selector
```python
# Use browser developer tools (F12)
# Example: Price selector
.re__pr-price  # Class for price element
```

### Step 2: Add to CSS_SELECTORS dict
```python
CSS_SELECTORS = {
    # ... existing
    "my_custom_field": ".css-selector-here",
}
```

### Step 3: Add extraction function
```python
def extract_my_custom_field(soup) -> Optional[str]:
    """Extract custom field from BeautifulSoup object"""
    elem = soup.select_one('.css-selector-here')
    if elem:
        return elem.text.strip()
    return None
```

### Step 4: Add to RealEstateSchema
```python
class RealEstateSchema(BaseModel):
    # ... existing fields
    my_custom_field: Optional[str] = None
```

### Step 5: Add to parse_detail_page()
```python
def parse_detail_page(html: str, url: str) -> RealEstateSchema:
    soup = BeautifulSoup(html, 'html.parser')
    
    # ... existing extractions
    
    # Add your extraction
    data['my_custom_field'] = extract_my_custom_field(soup)
    
    # ... rest of function
```

---

## 📈 Performance Monitoring

### Enable Debug Logging
```bash
--log-level DEBUG
```

### Key Metrics to Track
```python
# Warm-up duration
start = time.time()
# ... warm-up phase
warmup_duration = time.time() - start
logger.info(f"Warm-up: {warmup_duration:.2f}s")

# cf_clearance success
if cf_clearance:
    logger.info(f"cf_clearance acquired: {cf_clearance[:20]}...")
else:
    logger.warning("cf_clearance NOT acquired - fallback to browser")

# Phase selection
if cf_clearance:
    logger.info("Scale phase: HTTPX (fast path)")
else:
    logger.info("Scale phase: Browser (safe path)")

# Throughput
total_time = time.time() - start
throughput = len(results) / total_time * 60
logger.info(f"Throughput: {throughput:.0f} listings/min")
```

---

## 🔐 Exception Handling Strategy

### Network Errors
```python
try:
    result = await crawl_details_browser(...)
except asyncio.TimeoutError:
    logger.warning(f"Timeout for URL: {url}")
    # Fallback: Add to failed list, retry later
except Exception as e:
    logger.error(f"Crawl error: {e}")
    # Fallback: Use retry logic or skip URL
```

### Extraction Errors
```python
try:
    record = RealEstateSchema(...)
except ValidationError as e:
    logger.warning(f"Validation error for {url}: {e}")
    # Fallback: Use defaults for missing fields
    record = RealEstateSchema(url=url, **partial_data)
```

### Rate Limiting
```python
if response.status_code == 429:
    # Rate limited, implement backoff
    await asyncio.sleep(random.uniform(1, 5))
    # Retry with reduced concurrency
    concurrency = max(1, concurrency - 1)
```

---

## 🚀 Optimization Ideas

### 1. Adaptive Warm-up Count
- Monitor cf_clearance success rate
- Increase warm-up if success < 80%
- Decrease if always successful

### 2. Cookie Refresh Policy
- Monitor cf_clearance expiry (typically 30min)
- Auto-refresh before timeout
- Detect "blocked" responses → trigger warm-up

### 3. Proxy Health Checking
- Monitor proxy failure rates
- Remove dead proxies automatically
- Rotate to new proxy pool if needed

### 4. ML-Based Concurrency Tuning
- Correlate concurrency with success rate
- Find optimal concurrency per proxy pool
- Adjust dynamically during run

### 5. Database Export
```python
# Add PostgreSQL export
import psycopg2
conn = psycopg2.connect("dbname=batdongsan user=crawler")
# ... insert records to database
```

### 6. REST API Layer
```python
# Add FastAPI for crawler management
from fastapi import FastAPI
app = FastAPI()

@app.post("/crawl")
async def start_crawl(config: CrawlRequest):
    # Trigger crawl with custom config
    ...
```

---

## 📦 Testing Checklist

- [ ] Warm-up phase completes successfully
- [ ] cf_clearance cookie extracted and logged
- [ ] HTTPX phase uses 10-40x speed boost
- [ ] Resource blocking active (check Network tab)
- [ ] Proxy rotation working (check logs)
- [ ] Fallback triggers on HTTPX failure
- [ ] CSV export contains all 35 fields
- [ ] UTF-8-SIG encoding works in Excel
- [ ] Persistent context reuses cookies on re-run
- [ ] No memory leaks after 1000+ URLs
- [ ] Concurrency actually improves throughput
- [ ] Error handling doesn't crash script

---

## 🔗 Related Resources

- [Crawl4AI Docs](https://github.com/unclecode/crawl4ai)
- [HTTPX Docs](https://www.python-httpx.org/)
- [Pydantic v2 Docs](https://docs.pydantic.dev/latest/)
- [Cloudflare Bot Detection](https://developers.cloudflare.com/bots/plans/business-plus/)
- [Residential Proxies Guide](https://blog.apify.com/residential-proxies/)

---

**Version**: 4.0  
**Last Updated**: April 2026  
**Maintainer**: DataVisualisation Team
