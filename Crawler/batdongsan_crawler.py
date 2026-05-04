from __future__ import annotations

import argparse
import asyncio
import csv
import inspect
import itertools
import json
import logging
import random
import re
import signal
import unicodedata
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Callable, Optional
from urllib.parse import urljoin, urlparse
from urllib.request import Request, urlopen

import httpx
from bs4 import BeautifulSoup
from crawl4ai import AsyncWebCrawler, BrowserConfig, CacheMode, CrawlerRunConfig, MemoryAdaptiveDispatcher
from pydantic import BaseModel, ConfigDict, Field, ValidationError

try:
    from crawl4ai import ProxyConfig
except Exception:
    ProxyConfig = None


DEFAULT_START_URL = "https://batdongsan.com.vn/ban-can-ho-chung-cu/p1?cIds=650,362,44&vrs=1"
DEFAULT_OUTPUT = "batdongsan_real_estate.csv"
DEFAULT_MAX_PAGES = 200
DEFAULT_DELAY_MIN = 0.7
DEFAULT_DELAY_MAX = 1.8
DEFAULT_TIMEOUT_MS = 90_000
DEFAULT_WARMUP_COUNT = 3
DEFAULT_CONCURRENCY = 4
DEFAULT_MAX_SESSION_QUERIES = 120
DEFAULT_USER_DATA_DIR = ".crawler_profile"
DEFAULT_PROXY_FILE = "proxies.txt"
DEFAULT_HTTPX_TIMEOUT = 25.0
DEFAULT_CONCURRENT_REQUESTS_PER_HOST = 1
DEFAULT_PROGRESS_FILE = "progress.json"
DEFAULT_AUTOSAVE_BATCH_SIZE = 80

DEFAULT_MAX_RETRIES = 5
DEFAULT_RETRY_BASE_BACKOFF = 2.0
# Current effective settings (overridden from CLI in main)
CURRENT_MAX_RETRIES = DEFAULT_MAX_RETRIES
CURRENT_RETRY_BASE_BACKOFF = DEFAULT_RETRY_BASE_BACKOFF
STOP_REQUESTED = False

DETAIL_URL_PATTERN = re.compile(r"/[^\s?#]*-pr\d+(?:\?|$)", re.IGNORECASE)
PRODUCT_ID_PATTERN = re.compile(r"pr(\d+)", re.IGNORECASE)
DATE_PATTERN = re.compile(r"(\d{2}/\d{2}/\d{4})")

USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36"
)

# Small pool of user agents to rotate between sessions for variability
USER_AGENTS = [
    USER_AGENT,
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.0 Safari/605.1.15",
]

BLOCKED_EXTENSIONS = [".png", ".jpg", ".jpeg", ".gif", ".svg", ".woff2", ".ttf", ".css"]
BLOCKED_RESOURCE_TYPES = ["image", "font", "stylesheet"]


class RealEstateSchema(BaseModel):
    model_config = ConfigDict(populate_by_name=True, extra="ignore")

    product_id: str = Field(..., alias="prid")
    title: str = Field(..., description="Tiêu đề tin đăng")
    url: str
    city: str = Field(..., description="Tỉnh/Thành phố")
    district: str = Field(..., description="Quận/Huyện")
    ward: Optional[str] = Field(None, description="Phường/Xã")
    street: Optional[str] = Field(None, description="Đường/Phố")
    full_address: str
    price_total: float = Field(..., description="Giá tổng (triệu VNĐ)")
    price_per_m2: float = Field(..., description="Giá trên m2 (triệu VNĐ/m2)")
    price_min_range: Optional[float] = Field(None, description="Giá tối thiểu khoảng giá (triệu)")
    price_max_range: Optional[float] = Field(None, description="Giá tối đa khoảng giá (triệu)")
    area: float = Field(..., description="Diện tích công nhận (m2)")
    frontage: Optional[float] = Field(None, description="Mặt tiền (m)")
    road_width: Optional[float] = Field(None, description="Độ rộng đường vào (m)")
    bedrooms: Optional[int] = Field(0, description="Số phòng ngủ")
    bathrooms: Optional[int] = Field(0, description="Số phòng tắm")
    floors: Optional[int] = Field(1, description="Số tầng")
    direction: Optional[str] = Field(None, description="Hướng nhà")
    interior: Optional[str] = Field(None, description="Tình trạng nội thất")
    legal_status: str = Field("Sổ hồng/Sổ đỏ", description="Tình trạng pháp lý")
    post_rank: Optional[str] = Field(None, description="Cấp bậc tin (Tin VIP, Tin Nổi bật, ...)")
    price_trend_1y: Optional[float] = Field(None, description="Tăng trưởng giá khu vực (%)")
    is_verified: bool = Field(False, description="Tin đã được xác thực bởi sàn")
    has_elevator: bool = Field(False, description="Có thang máy")
    near_park: bool = Field(False, description="Gần công viên/bờ sông")
    is_frontage_road: bool = Field(False, description="Mặt tiền đường")
    description: str = Field(..., description="Toàn bộ nội dung mô tả")
    posted_date: date
    expiry_date: date


def normalize_text(text: str) -> str:
    return re.sub(r"\s+", " ", text).strip()


def normalize_lines(text: str) -> list[str]:
    return [normalize_text(line) for line in text.splitlines() if normalize_text(line)]


def strip_accents(text: str) -> str:
    normalized = unicodedata.normalize("NFKD", text)
    return "".join(char for char in normalized if not unicodedata.combining(char))


def canonical(text: str) -> str:
    return strip_accents(normalize_text(text)).lower()


STREET_PREFIX_PATTERN = re.compile(
    r"^(?:duong|hem|pho|ngo|ngach)\b|^(?:quoc lo|tinh lo)\b|^(?:ql|tl|dt)(?:\b|\d)",
    re.IGNORECASE,
)


def extract_street_from_parts(parts: list[str]) -> Optional[str]:
    for part in parts:
        part_canon = canonical(part).replace("đ", "d")
        if STREET_PREFIX_PATTERN.search(part_canon):
            return normalize_text(part)
    return None


def make_absolute_url(base_url: str, href: str) -> str:
    return urljoin(base_url, href)


def filter_supported_kwargs(target: Any, kwargs: dict[str, Any]) -> dict[str, Any]:
    try:
        parameters = inspect.signature(target).parameters
    except (TypeError, ValueError):
        return kwargs
    return {key: value for key, value in kwargs.items() if key in parameters}


def load_proxies(proxy_file: Path) -> list[str]:
    if not proxy_file.exists():
        return []
    proxies: list[str] = []
    for line in proxy_file.read_text(encoding="utf-8").splitlines():
        candidate = line.strip()
        if not candidate or candidate.startswith("#"):
            continue
        proxies.append(candidate)
    return proxies


def build_proxy_config_dict(proxy_url: Optional[str]) -> Optional[dict[str, str]]:
    """Build proxy config dict for httpx (not for BrowserConfig)."""
    if not proxy_url:
        return None
    return {"all://": proxy_url}


def build_browser_config(
    *,
    user_data_dir: Path,
    proxy_url: Optional[str] = None,
    use_persistent_context: bool = True,
    headful: bool = False,
    user_agent: Optional[str] = None,
) -> BrowserConfig:
    extra_args = ["--disable-blink-features=AutomationControlled"]
    # Only set headless arg when running headless; allow headful mode to be more stealthy
    if not headful:
        extra_args.append("--headless=new")

    config_args = {
        "headless": not headful,
        "enable_stealth": True,
        "stealth": True,
        "extra_args": extra_args,
        "user_agent": user_agent or USER_AGENT,
        "use_persistent_context": use_persistent_context,
        "user_data_dir": str(user_data_dir),
        # Prefer the newer ProxyConfig API when available; fall back to the legacy string
        # parameter for older crawl4ai versions.
    }
    if proxy_url:
        if ProxyConfig is not None:
            proxy_obj = None
            try:
                # Try common constructors: from_url() or direct constructor
                if hasattr(ProxyConfig, "from_url"):
                    proxy_obj = ProxyConfig.from_url(proxy_url)
                else:
                    # Fallback: attempt to construct with a single arg or a 'url' kwarg
                    try:
                        proxy_obj = ProxyConfig(proxy_url)
                    except Exception:
                        try:
                            proxy_obj = ProxyConfig(url=proxy_url)
                        except Exception:
                            proxy_obj = None
            except Exception:
                proxy_obj = None

            if proxy_obj is not None:
                config_args["proxy_config"] = proxy_obj
            else:
                config_args["proxy"] = proxy_url
        else:
            config_args["proxy"] = proxy_url
    filtered = filter_supported_kwargs(BrowserConfig.__init__, config_args)
    return BrowserConfig(**filtered)


def build_dispatcher(max_session_queries: int) -> MemoryAdaptiveDispatcher:
    dispatcher_args = filter_supported_kwargs(
        MemoryAdaptiveDispatcher.__init__,
        {
            "max_session_queries": max_session_queries,
            "max_concurrent_sessions": DEFAULT_CONCURRENCY,
            "max_concurrent": DEFAULT_CONCURRENCY,
        },
    )
    return MemoryAdaptiveDispatcher(**dispatcher_args)


# JS expression that waits until the Cloudflare challenge page is gone and real
# page content is present.  Returns True when the page has navigated past any
# CF interstitial.  Used as the ``wait_for`` parameter in CrawlerRunConfig.
CF_WAIT_JS = """() => {
    // If we're still on a CF challenge page, keep waiting
    const title = document.title || '';
    const body = document.body ? document.body.innerText : '';
    const isCF = title.toLowerCase().includes('just a moment')
              || body.toLowerCase().includes('cf-challenge')
              || body.toLowerCase().includes('attention required')
              || !!document.getElementById('cf-challenge-running');
    return !isCF;
}"""


def build_run_config(dispatcher: Optional[MemoryAdaptiveDispatcher] = None) -> CrawlerRunConfig:
    blocked_url_patterns = [rf".*{re.escape(ext)}(\?.*)?$" for ext in BLOCKED_EXTENSIONS]
    config_args = {
        "cache_mode": CacheMode.BYPASS,
        "wait_until": "domcontentloaded",
        "page_timeout": DEFAULT_TIMEOUT_MS,
        "max_retries": DEFAULT_MAX_RETRIES,
        "magic": True,
        "simulate_user": True,
        "scan_full_page": False,
        "wait_for_images": False,
        "delay_before_return_js": 2.0,
        "blocked_url_patterns": blocked_url_patterns,
        "blocked_resource_types": BLOCKED_RESOURCE_TYPES,
        "dispatcher": dispatcher,
        "wait_for": CF_WAIT_JS,
    }
    filtered = filter_supported_kwargs(CrawlerRunConfig.__init__, config_args)
    return CrawlerRunConfig(**filtered)


async def validate_proxy_async(proxy_url: str, test_url: str, timeout: float = 10.0) -> bool:
    """Quickly test an HTTP proxy using httpx. Returns True when proxy successfully fetches test_url."""
    proxies = build_proxy_config_dict(proxy_url)
    try:
        async with httpx.AsyncClient(proxies=proxies, timeout=timeout, follow_redirects=True) as client:
            resp = await client.get(test_url)
            return resp.status_code == 200
    except Exception:
        return False


def build_httpx_headers(user_agent: str) -> dict[str, str]:
    return {
        "User-Agent": user_agent,
        "Accept-Language": "vi-VN,vi;q=0.9,en-US;q=0.8,en;q=0.7",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Cache-Control": "no-cache",
        "Pragma": "no-cache",
    }


def is_cf_challenge(html: str) -> bool:
    """Check if the HTML is a Cloudflare challenge page.
    Must avoid false positives on real pages that mention 'cloudflare'
    in feature flags or scripts (e.g., 'cloudflare-turnstile-viewphone').
    """
    if not html:
        return True
    # Real CF challenge pages are very short (<10KB) and have specific markers.
    # Real batdongsan pages are 100KB+ and may contain 'cloudflare' in feature configs.
    html_lower = html.lower()
    # Strong indicators of an actual CF challenge interstitial
    strong_markers = [
        "<title>just a moment</title>",
        "cf-challenge-running",
        "cf_chl_opt",
        "cf-challenge-hcaptcha",
        "attention required! | cloudflare",
        'id="challenge-form"',
        'id="challenge-running"',
    ]
    if any(marker in html_lower for marker in strong_markers):
        return True
    # If the page is very short and contains generic CF references, it's likely a challenge
    if len(html) < 15000:
        weak_markers = ["cf-challenge", "attention required"]
        if any(marker in html_lower for marker in weak_markers):
            return True
    return False


def _find_browser_context(crawler: AsyncWebCrawler):
    """Locate the Playwright BrowserContext from the crawler instance.
    crawl4ai 0.8.x stores it deep inside the strategy/manager hierarchy.
    """
    # Direct attributes (older crawl4ai versions)
    for attr in ("context", "browser_context", "_context"):
        ctx = getattr(crawler, attr, None)
        if ctx is not None:
            return ctx
    # crawl4ai 0.8.x: crawler.crawler_strategy.browser_manager.default_context
    strategy = getattr(crawler, "crawler_strategy", None)
    if strategy is not None:
        bm = getattr(strategy, "browser_manager", None)
        if bm is not None:
            ctx = getattr(bm, "default_context", None)
            if ctx is not None:
                return ctx
    return None


async def extract_session_cookies(crawler: AsyncWebCrawler) -> dict[str, str]:
    context = _find_browser_context(crawler)
    if context is None:
        return {}
    cookies_call = getattr(context, "cookies", None)
    if cookies_call is None:
        return {}
    cookies = cookies_call()
    if inspect.isawaitable(cookies):
        cookies = await cookies
    cookie_map: dict[str, str] = {}
    for cookie in cookies or []:
        name = cookie.get("name")
        value = cookie.get("value")
        if name and value:
            cookie_map[name] = value
    return cookie_map


def log_cf_clearance(cookies: dict[str, str]) -> None:
    cf_cookie = cookies.get("cf_clearance")
    if not cf_cookie:
        logging.warning("cf_clearance cookie not found after warm-up")
        return
    logging.info("cf_clearance acquired: %s...", cf_cookie[:6])


def extract_html(result: Any) -> str:
    for attribute in ("html", "cleaned_html", "raw_html", "source_html"):
        value = getattr(result, attribute, None)
        if isinstance(value, str) and value.strip():
            return value
    markdown = getattr(result, "markdown", None)
    if markdown is not None:
        raw_markdown = getattr(markdown, "raw_markdown", None)
        if isinstance(raw_markdown, str) and raw_markdown.strip():
            return raw_markdown
    return ""


def extract_text_from_html(html: str) -> str:
    soup = BeautifulSoup(html, "html.parser")
    return normalize_text(soup.get_text("\n", strip=True))


def extract_product_id(url: str, text: str = "") -> str:
    for source in (url, text):
        match = PRODUCT_ID_PATTERN.search(source or "")
        if match:
            return match.group(1)
    return ""


def clean_numeric(text: str) -> Optional[float]:
    """
    Helper: Trích xuất số từ chuỗi text và xử lý đơn vị.
    Xử lý các định dạng: "45,65", "45.65", "45,65 tỷ", "~194,44 triệu/m²"
    Chuyển đổi dấu phẩy thành dấu chấm trước khi convert sang float.
    
    Returns: float value without unit (e.g., 45.65, 194.44)
    """
    if not text:
        return None
    
    # Loại bỏ ký tự đặc biệt (~ dấu gạch ngang, khoảng trắng)
    text = re.sub(r"[~\s]+", "", text).strip()
    
    # Trích xuất số
    match = re.search(r"-?[\d.,]+", text)
    if not match:
        return None
    
    value = match.group(0)
    
    # Xử lý dấu phẩy và chấm
    if "," in value and "." in value:
        # Nếu phẩy đứng sau chấm: "1.234,56" → "1234.56"
        if value.rfind(",") > value.rfind("."):
            value = value.replace(".", "").replace(",", ".")
        # Nếu chấm đứng sau phẩy: "1,234.56" → "1234.56"
        else:
            value = value.replace(",", "")
    elif "," in value:
        # Chỉ có phẩy: "194,44" → "194.44"
        value = value.replace(",", ".")
    # Nếu chỉ có chấm hoặc không có gì: giữ nguyên
    
    try:
        return float(value)
    except ValueError:
        return None


def safe_decimal(raw: str) -> Optional[float]:
    """Alias của clean_numeric() để tương thích ngược."""
    return clean_numeric(raw)


def parse_money_total(raw: str) -> Optional[float]:
    value = safe_decimal(raw)
    if value is None:
        return None
    raw_canon = canonical(raw)
    if "trieu" in raw_canon or re.search(r"\btr\.?/", raw_canon):
        return value / 1000.0
    if "ty" in raw_canon:
        return value
    return value


def parse_money_per_m2(raw: str) -> Optional[float]:
    value = safe_decimal(raw)
    if value is None:
        return None
    raw_canon = canonical(raw)
    if "trieu" in raw_canon or re.search(r"\btr\.?/", raw_canon):
        return value
    if "ty" in raw_canon:
        return value * 1000.0
    return value


def parse_date(raw: str) -> Optional[date]:
    match = DATE_PATTERN.search(raw)
    if not match:
        return None
    return datetime.strptime(match.group(1), "%d/%m/%Y").date()


def find_line_containing(lines: list[str], keywords: list[str]) -> Optional[str]:
    canonical_keywords = [canonical(keyword) for keyword in keywords]
    for line in lines:
        line_canon = canonical(line)
        if all(keyword in line_canon for keyword in canonical_keywords):
            return line
    return None


def find_line_after_label(lines: list[str], label_candidates: list[str]) -> Optional[str]:
    canonical_labels = [canonical(label) for label in label_candidates]
    for index, line in enumerate(lines):
        line_canon = canonical(line)
        for label_canon in canonical_labels:
            if line_canon == label_canon:
                for follow in lines[index + 1 : index + 4]:
                    if follow:
                        return follow
            if line_canon.startswith(label_canon + " "):
                return line_canon[len(label_canon) :].strip(" :-") or line
    return None


def first_match(text: str, patterns: list[str], flags: int = re.IGNORECASE) -> Optional[str]:
    for pattern in patterns:
        match = re.search(pattern, text, flags)
        if match:
            return match.group(1) if match.groups() else match.group(0)
    return None


def parse_location(lines: list[str], page_title: str) -> dict[str, Optional[str]]:
    title_index = next((i for i, line in enumerate(lines) if canonical(line) == canonical(page_title)), -1)
    search_space = lines[title_index + 1 : title_index + 10] if title_index >= 0 else lines[:20]

    candidate = None
    for line in search_space:
        line_canon = canonical(line)
        if "," in line and any(token in line_canon for token in ("phuong", "xa", "quan", "huyen", "duong", "ho chi minh", "ha noi")):
            candidate = line
            break

    if candidate is None:
        for line in lines:
            line_canon = canonical(line)
            if "," in line and any(token in line_canon for token in ("quan", "huyen", "phuong", "xa")):
                candidate = line
                break

    if candidate is None:
        breadcrumb = find_line_containing(lines, ["Bán/"]) or ""
        candidate = breadcrumb or page_title

    parts = [normalize_text(part) for part in candidate.split(",") if normalize_text(part)]
    full_address = candidate
    city = None
    district = None
    ward = None
    street = extract_street_from_parts(parts)

    for part in parts:
        part_canon = canonical(part)
        if city is None and any(token in part_canon for token in ("ho chi minh", "hcm", "ha noi", "da nang", "hai phong", "can tho", "binh duong", "dong nai", "khanh hoa", "lam dong")):
            city = part.replace("TP.", "").replace("Tp.", "").strip()
        if district is None and any(token in part_canon for token in ("quan", "huyen", "thi xa", "thanh pho")):
            district = part
        if ward is None and any(token in part_canon for token in ("phuong", "xa", "thi tran")):
            ward = part

    if city is None and len(parts) >= 2:
        city = parts[-1]
    if district is None and len(parts) >= 2:
        district = parts[-2]
    if street is None:
        street = extract_street_from_parts(parts)

    return {
        "full_address": full_address,
        "city": city or "",
        "district": district or "",
        "ward": ward,
        "street": street,
    }


def parse_address_improved(full_address: str) -> dict[str, Optional[str]]:
    """
    Xử lý địa chỉ đầu vào thành 4 trường riêng biệt.
    Format mong đợi: "Đường D4, Phường Phú Mỹ, Quận 7, Hồ Chí Minh"
    """
    parts = [normalize_text(part) for part in full_address.split(",") if normalize_text(part)]
    
    if not parts:
        return {"city": "", "district": "", "ward": None, "street": None, "full_address": full_address}
    
    # Khởi tạo các giá trị mặc định
    city = None
    district = None
    ward = None
    street = extract_street_from_parts(parts)
    
    # Duyệt ngược từ cuối để tìm city, district, ward
    for i, part in enumerate(reversed(parts)):
        part_canon = canonical(part)
        
        # Tìm thành phố (từ cuối cùng)
        if city is None and any(token in part_canon for token in 
            ("ho chi minh", "hcm", "ha noi", "da nang", "hai phong", "can tho", 
             "binh duong", "dong nai", "khanh hoa", "lam dong", "tp.", "tphcm")):
            city = part.replace("TP.", "").replace("Tp.", "").replace("TP", "").strip()
        
        # Tìm quận/huyện
        if district is None and any(token in part_canon for token in ("quan", "huyen", "thi xa", "thanh pho")):
            district = part
        
        # Tìm phường/xã
        if ward is None and any(token in part_canon for token in ("phuong", "xa", "thi tran")):
            ward = part
    
    # Fallback: nếu không tìm được city, lấy phần cuối cùng
    if city is None and len(parts) > 0:
        city = parts[-1]
    
    # Fallback: nếu không tìm được district, lấy phần áp chót
    if district is None and len(parts) > 1:
        district = parts[-2]
    
    return {
        "city": city or "",
        "district": district or "",
        "ward": ward,
        "street": street,
        "full_address": full_address
    }


def parse_numeric_features(text: str, lines: list[str]) -> dict[str, Any]:
    price_total_raw = first_match(
        text,
        [
            r"Khoảng giá\s*\n\s*([\d.,]+\s*tỷ)",
            r"Giá\s*\n\s*([\d.,]+\s*tỷ)",
            r"([\d.,]+\s*tỷ)",
            r"([\d.,]+\s*triệu)",
        ],
    )
    price_per_m2_raw = first_match(
        text,
        [r"([\d.,]+\s*triệu\s*/\s*m²)", r"([\d.,]+\s*tr\.?\s*/\s*m²)", r"([\d.,]+\s*tr/m²)"]
    )
    area_raw = first_match(text, [r"Diện tích\s*\n\s*([\d.,]+\s*m²)", r"([\d.,]+\s*m²)"])
    frontage_raw = first_match(text, [r"Mặt tiền\s*([\d.,]+)\s*m", r"Mặt tiền\s*([\d.,]+)\s*m", r"Ngang\s*([\d.,]+)\s*m"])
    road_width_raw = first_match(text, [r"Đường vào\s*([\d.,]+)\s*m", r"Đường vào\s*([\d.,]+)\s*m", r"Hẻm\s*([\d.,]+)\s*m"])
    bedrooms_raw = first_match(text, [r"(\d+)\s*PN\b", r"Phòng ngủ\s*([\d]+)", r"(\d+)\s*phòng ngủ"])
    bathrooms_raw = first_match(text, [r"(\d+)\s*WC\b", r"(\d+)\s*toilet\b", r"Phòng tắm\s*([\d]+)"])
    floors_raw = first_match(text, [r"(\d+)\s*tầng", r"(\d+)\s*lầu", r"(\d+)\s*floor"])
    direction_raw = first_match(
        text,
        [
            r"Hướng nhà\s*([A-Za-zÀ-Ỵà-ỵ]+(?:\s*-\s*[A-Za-zÀ-Ỵà-ỵ]+)?(?:\s+[A-Za-zÀ-Ỵà-ỵ]+)?)\s*(?=Hướng ban công|Pháp lý|Nội thất|$)",
            r"Hướng nhà\s*([A-Za-zÀ-Ỵà-ỵ]+(?:\s*-\s*[A-Za-zÀ-Ỵà-ỵ]+)?(?:\s+[A-Za-zÀ-Ỵà-ỵ]+)?)",
        ],
    )
    legal_status_raw = find_line_after_label(lines, ["Pháp lý", "Phap ly"])
    posted_date_raw = first_match(text, [r"Ngày đăng\s*([0-3]?\d/[0-1]?\d/\d{4})"])
    expiry_date_raw = first_match(text, [r"Ngày hết hạn\s*([0-3]?\d/[0-1]?\d/\d{4})"])

    if posted_date_raw is None:
        for index, line in enumerate(lines):
            if canonical(line) == "ngay dang":
                posted_date_raw = next((candidate for candidate in lines[index + 1 : index + 4] if DATE_PATTERN.search(candidate)), None)
                break

    if expiry_date_raw is None:
        for index, line in enumerate(lines):
            if canonical(line) == "ngay het han":
                expiry_date_raw = next((candidate for candidate in lines[index + 1 : index + 4] if DATE_PATTERN.search(candidate)), None)
                break

    price_total = parse_money_total(price_total_raw or "") if price_total_raw else None
    price_per_m2 = parse_money_per_m2(price_per_m2_raw or "") if price_per_m2_raw else None
    area = safe_decimal(area_raw or "") if area_raw else None
    frontage = safe_decimal(frontage_raw or "") if frontage_raw else None
    road_width = safe_decimal(road_width_raw or "") if road_width_raw else None
    bedrooms = int(float(bedrooms_raw)) if bedrooms_raw else None
    bathrooms = int(float(bathrooms_raw)) if bathrooms_raw else None
    floors = int(float(floors_raw)) if floors_raw else None
    direction = normalize_text(direction_raw) if direction_raw else None
    legal_status = normalize_text(legal_status_raw) if legal_status_raw else "Sổ hồng/Sổ đỏ"

    return {
        "price_total": price_total,
        "price_per_m2": price_per_m2,
        "area": area,
        "frontage": frontage,
        "road_width": road_width,
        "bedrooms": bedrooms,
        "bathrooms": bathrooms,
        "floors": floors,
        "direction": direction,
        "legal_status": legal_status,
        "posted_date": parse_date(posted_date_raw or "") if posted_date_raw else None,
        "expiry_date": parse_date(expiry_date_raw or "") if expiry_date_raw else None,
    }


def extract_description(text: str, page_title: str) -> str:
    lines = normalize_lines(text)
    start_index = next((i for i, line in enumerate(lines) if canonical(line) == "thong tin mo ta"), -1)
    if start_index < 0:
        return text

    stop_markers = {
        canonical("Xem thêm lịch sử giá"),
        canonical("Người đăng"),
        canonical("Ngày đăng"),
        canonical("Loại tin"),
        canonical("Mã tin"),
        canonical("Bất động sản dành cho bạn"),
        canonical("Tin đăng đã xem"),
        canonical("Tìm kiếm theo từ khóa"),
        canonical("Xem trên bản đồ"),
    }

    collected: list[str] = []
    for line in lines[start_index + 1 :]:
        if canonical(line) in stop_markers:
            break
        collected.append(line)
    description = "\n".join(collected).strip()
    return description or page_title


def extract_price_range(html: str) -> tuple[Optional[float], Optional[float]]:
    """
    Trích xuất giá min/max từ <div class="meter-range">.
    Ví dụ: <div class="meter-range"><span class="min">64</span>...<span class="max">206</span></div>
    Trả về giá trị đã convert sang triệu đồng.
    """
    soup = BeautifulSoup(html, "html.parser")
    meter_range = soup.select_one(".meter-range")
    
    if not meter_range:
        return None, None
    
    min_elem = meter_range.select_one(".min")
    max_elem = meter_range.select_one(".max")
    
    price_min = None
    price_max = None
    
    if min_elem:
        min_text = normalize_text(min_elem.get_text())
        price_min = safe_decimal(min_text)
    
    if max_elem:
        max_text = normalize_text(max_elem.get_text())
        price_max = safe_decimal(max_text)
    
    return price_min, price_max


def extract_specs_from_content(html: str) -> dict[str, Any]:
    """
    Bóc tách đặc điểm bất động sản từ danh sách .re__pr-specs-content-item.
    Mỗi item có: <div class="re__pr-specs-content-item" title="...">
    """
    soup = BeautifulSoup(html, "html.parser")
    specs = {
        "bedrooms": None,
        "bathrooms": None,
        "floors": None,
        "direction": None,
        "frontage": None,
        "road_width": None,
        "legal_status": None,
        "interior": None,
    }
    
    spec_items = soup.select(".re__pr-specs-content-item")
    
    for item in spec_items:
        # Ưu tiên đọc đúng title/value từ các span con để tránh dính text của item kế bên.
        title_elem = item.select_one(".re__pr-specs-content-item-title")
        value_elem = item.select_one(".re__pr-specs-content-item-value")

        title = (normalize_text(title_elem.get_text()) if title_elem else item.get("title", "")).lower()
        value = normalize_text(value_elem.get_text()) if value_elem else normalize_text(item.get_text())
        
        if "số phòng ngủ" in title or "bedroom" in title:
            specs["bedrooms"] = int(safe_decimal(value) or 0)
        elif "số phòng tắm, vệ sinh" in title or "vệ sinh" in title or "bathroom" in title:
            specs["bathrooms"] = int(safe_decimal(value) or 0)
        elif "số tầng" in title or "lầu" in title or "floor" in title:
            specs["floors"] = int(safe_decimal(value) or 1)
        elif "hướng nhà" in title or "direction" in title:
            specs["direction"] = value
        elif "mặt tiền" in title or "frontage" in title:
            specs["frontage"] = safe_decimal(value)
        elif "đường vào" in title or "hẻm" in title or "road" in title:
            specs["road_width"] = safe_decimal(value)
        elif "pháp lý" in title or "legal" in title:
            specs["legal_status"] = value
        elif "nội thất" in title or "interior" in title:
            specs["interior"] = value
    
    return specs


def extract_post_rank_from_info(html: str) -> Optional[str]:
    """
    Trích xuất cấp bậc tin từ .re__pr-short-info-item có title="Loại tin".
    Ví dụ: "Tin VIP Vàng", "Tin Nổi bật", ...
    """
    soup = BeautifulSoup(html, "html.parser")
    
    for item in soup.select(".re__pr-short-info-item"):
        title_elem = item.select_one(".title")
        value_elem = item.select_one(".value")
        title = normalize_text(title_elem.get_text()) if title_elem else item.get("title", "")
        title_canon = canonical(title)

        if "loai tin" in title_canon or "post rank" in title_canon:
            value = normalize_text(value_elem.get_text()) if value_elem else normalize_text(item.get_text())
            if value:
                return value
    
    return None


def extract_description_flags(description: str) -> dict[str, bool]:
    """
    Trích xuất các flag Boolean từ mô tả.
    - has_elevator: True nếu có từ khóa "thang máy"
    - near_park: True nếu có từ khóa "công viên", "bờ sông", "view thông thoáng"
    - is_frontage_road: True nếu có từ khóa "mặt tiền đường"
    """
    desc_canon = canonical(description)
    
    return {
        "has_elevator": bool(re.search(r"thang\s*may|elevat", desc_canon)),
        "near_park": bool(re.search(r"cong\s*vien|bo\s*song|ho\s*nuoc|view\s*thong\s*thoang", desc_canon)),
        "is_frontage_road": bool(re.search(r"mat\s*tien\s*duong|mat\s*duong", desc_canon)),
    }


def extract_address_from_selector(html: str) -> Optional[str]:
    """
    Trích xuất full_address từ .re__address-line-1
    Ví dụ: "Đường D4, Phường Phú Mỹ, Quận 7, Hồ Chí Minh"
    """
    soup = BeautifulSoup(html, "html.parser")
    address_elem = soup.select_one(".re__address-line-1")
    
    if address_elem:
        return normalize_text(address_elem.get_text())
    
    return None


def extract_product_id_from_selector(html: str, url: str) -> Optional[str]:
    """
    Trích xuất product_id từ:
    1. Attribute prid của tag #product-detail-web
    2. Fallback: extract từ URL
    """
    soup = BeautifulSoup(html, "html.parser")
    
    # Try #product-detail-web prid attribute
    product_elem = soup.select_one("#product-detail-web")
    if product_elem:
        prid = product_elem.get("prid")
        if prid:
            return str(prid).strip()
    
    # Fallback: extract từ URL
    match = PRODUCT_ID_PATTERN.search(url)
    if match:
        return match.group(1)
    
    return None


def extract_price_total_from_selector(html: str) -> Optional[float]:
    """
    Trích xuất price_total từ .re__pr-short-info-item.
    Tìm item chứa giá (ví dụ: "~45,65 tỷ").
    Nếu đơn vị là "tỷ", nhân với 1000 để convert sang triệu.
    """
    soup = BeautifulSoup(html, "html.parser")
    
    # Tìm các short-info items
    for item in soup.select(".re__pr-short-info-item"):
        value_text = normalize_text(item.get_text())
        
        # Bỏ qua các items không có giá
        if not any(char.isdigit() for char in value_text):
            continue
        
        # Bỏ qua "Loại tin", "Mã tin", v.v.
        title = item.get("title", "").lower()
        if any(keyword in title for keyword in ("loai tin", "ma tin", "ngay dang")):
            continue
        
        # Trích xuất số
        amount = clean_numeric(value_text)
        if amount is None:
            continue
        
        # Kiểm tra đơn vị là tỷ hay triệu
        value_canon = canonical(value_text)
        if "ty" in value_canon:
            # Convert tỷ → triệu (nhân 1000)
            return amount * 1000.0
        elif "trieu" in value_canon or "tr" in value_canon:
            # Đã là triệu
            return amount
    
    return None


def extract_price_per_m2_from_selector(html: str) -> Optional[float]:
    """
    Trích xuất price_per_m2 từ .re__pr-short-info-item.
    Tìm item chứa "triệu/m²" hoặc "/m²".
    """
    soup = BeautifulSoup(html, "html.parser")
    
    for item in soup.select(".re__pr-short-info-item"):
        value_text = normalize_text(item.get_text())
        
        # Tìm item có "/m²" hoặc "m²"
        if "/m²" in value_text or "/m2" in value_text:
            amount = clean_numeric(value_text)
            if amount is not None:
                # Đã là triệu/m²
                return amount
    
    return None


def extract_area_and_frontage_from_selector(html: str) -> tuple[Optional[float], Optional[float]]:
    """
    Trích xuất area và frontage.
    - area: Lấy từ item có title "Diện tích" trong .re__pr-specs-content-item
    - frontage: Lấy từ item có title "Mặt tiền" trong .re__pr-specs-content-item
    - Fallback: Lấy frontage từ span.ext chứa "Mặt tiền" (không chứa giá)
    """
    soup = BeautifulSoup(html, "html.parser")
    
    area = None
    frontage = None
    
    for item in soup.select(".re__pr-specs-content-item"):
        # Ưu tiên đọc title từ child span .re__pr-specs-content-item-title
        title_elem = item.select_one(".re__pr-specs-content-item-title")
        value_elem = item.select_one(".re__pr-specs-content-item-value")
        
        if title_elem:
            title_key = canonical(normalize_text(title_elem.get_text()))
        else:
            # Fallback: đọc từ HTML title attribute
            title_key = canonical(item.get("title", ""))
        
        raw_value = normalize_text(value_elem.get_text()) if value_elem else normalize_text(item.get_text())
        
        if "dien tich" in title_key:
            area = clean_numeric(raw_value)
        elif "mat tien" in title_key:
            frontage = clean_numeric(raw_value)
    
    # Fallback: Tìm frontage từ span.ext chứa "Mặt tiền"
    # Chỉ lấy span.ext mà text bắt đầu bằng "Mặt tiền" và KHÔNG chứa giá (triệu, tỷ, m²)
    if frontage is None:
        for ext_elem in soup.select("span.ext"):
            ext_text = normalize_text(ext_elem.get_text())
            ext_lower = ext_text.lower()
            # Bỏ qua nếu chứa giá tiền
            if any(kw in ext_lower for kw in ("triệu", "tỷ", "m²", "m2")):
                continue
            # Chỉ lấy nếu text chứa "Mặt tiền"
            if "mặt tiền" in ext_lower or "mat tien" in canonical(ext_text):
                frontage = clean_numeric(ext_text)
                break
    
    return area, frontage


def extract_post_rank_from_selector(html: str) -> Optional[str]:
    """
    Trích xuất post_rank từ item "Loại tin".
    Ví dụ: "Tin VIP Kim Cương", "Tin Nổi bật"
    """
    soup = BeautifulSoup(html, "html.parser")
    
    for item in soup.select(".re__pr-short-info-item"):
        title_elem = item.select_one(".title")
        value_elem = item.select_one(".value")
        title = normalize_text(title_elem.get_text()) if title_elem else item.get("title", "")
        title_canon = canonical(title)

        if "loai tin" in title_canon or "post rank" in title_canon:
            value_text = normalize_text(value_elem.get_text()) if value_elem else normalize_text(item.get_text())
            return value_text if value_text else None
    
    return None


def extract_price_trend_from_selector(html: str) -> Optional[float]:
    """
    Trích xuất price_trend_1y từ khối pricing CTA.
    Ví dụ: "1,3%", "+23%", "-5%"
    Trả về số (23.0, -5.0)
    """
    soup = BeautifulSoup(html, "html.parser")

    # Ưu tiên block pricing CTA mới (ví dụ: .re__up-trend/.re__down-trend)
    cta_block = soup.select_one(".re__block-ldp-pricing-cta")
    if cta_block:
        number_elem = cta_block.select_one(".cta-number")
        if number_elem:
            trend_text = normalize_text(number_elem.get_text())
            trend_value = clean_numeric(trend_text)
            if trend_value is not None:
                cta_classes = cta_block.get("class", [])
                # Giữ dấu âm nếu block thể hiện xu hướng giảm.
                if any("down-trend" in cls for cls in cta_classes):
                    return -abs(trend_value)
                return trend_value

    # Fallback selector cũ để tương thích ngược.
    chart_elem = soup.select_one(".re__chart-col.re__col-2 strong")
    if chart_elem:
        trend_text = normalize_text(chart_elem.get_text())
        trend_value = clean_numeric(trend_text)
        return trend_value
    
    return None


def extract_is_verified_from_selector(html: str) -> bool:
    """
    Kiểm tra xem tin đã được xác thực hay chưa.
    Returns True nếu có class .marking-product__KYC
    """
    soup = BeautifulSoup(html, "html.parser")
    
    kyc_elem = soup.select_one(".marking-product__KYC")
    return kyc_elem is not None


def parse_detail_page(url: str, html: str, fallback_title: str = "") -> RealEstateSchema:
    """
    Bóc tách dữ liệu chi tiết từ trang BĐS sử dụng cách tiếp cận hybrid:
    1. CSS selectors (ưu tiên)
    2. Regex (fallback)
    3. Text extraction (fallback)
    
    Merge strategy: CSS > Regex > Text
    """
    soup = BeautifulSoup(html, "html.parser")
    page_title = normalize_text(
        soup.find("h1").get_text(" ", strip=True)
        if soup.find("h1")
        else (soup.title.get_text(" ", strip=True) if soup.title else fallback_title)
    )
    page_title = re.sub(r"\s*\|\s*Batdongsan\.com\.vn.*$", "", page_title, flags=re.IGNORECASE)
    
    text = extract_text_from_html(html)
    lines = normalize_lines(text)
    
    # Trích xuất dữ liệu từ text (fallback)
    metrics = parse_numeric_features(text, lines)
    product_id_old = extract_product_id(url, text)
    description = extract_description(text, page_title)
    
    # === PHASE 1: CSS Selectors (Priority) ===
    full_address_css = extract_address_from_selector(html)
    product_id_css = extract_product_id_from_selector(html, url)
    price_total_css = extract_price_total_from_selector(html)
    price_per_m2_css = extract_price_per_m2_from_selector(html)
    area_css, frontage_css = extract_area_and_frontage_from_selector(html)
    post_rank_css = extract_post_rank_from_selector(html)
    price_trend_css = extract_price_trend_from_selector(html)
    is_verified_css = extract_is_verified_from_selector(html)
    
    # === PHASE 2: Regex Fallback (if CSS returns None) ===
    price_min, price_max = extract_price_range(html)
    specs = extract_specs_from_content(html)
    
    # Extract description flags
    desc_flags = extract_description_flags(description)
    
    # === MERGE STRATEGY: CSS > Regex > Text ===
    # Address
    full_address = full_address_css or metrics.get("full_address") or ""
    location_improved = parse_address_improved(full_address)
    location = parse_location(lines, page_title) if not location_improved["city"] else location_improved
    
    # Product ID
    product_id = product_id_css or product_id_old or ""
    
    # Prices
    price_total = price_total_css or metrics.get("price_total")
    price_per_m2 = price_per_m2_css or metrics.get("price_per_m2")
    
    # Area & Frontage
    area = area_css or metrics.get("area")
    frontage = frontage_css or specs.get("frontage") or metrics.get("frontage")
    
    # Specs từ HTML (CSS priority)
    bedrooms = specs.get("bedrooms") or metrics.get("bedrooms", 0)
    bathrooms = specs.get("bathrooms") or metrics.get("bathrooms", 0)
    floors = specs.get("floors") or metrics.get("floors", 1)
    direction = specs.get("direction") or metrics.get("direction")
    road_width = specs.get("road_width") or metrics.get("road_width")
    legal_status = specs.get("legal_status") or metrics.get("legal_status", "Sổ hồng/Sổ đỏ")
    interior = specs.get("interior")
    
    posted_date = metrics["posted_date"] or date.today()
    expiry_date = metrics["expiry_date"] or (posted_date + timedelta(days=30))

    # Validation
    if not product_id:
        raise ValueError(f"Could not extract product_id for {url}")
    if not location["city"] or not location["district"]:
        raise ValueError(f"Could not extract location for {url}")
    if price_total is None or price_per_m2 is None or area is None:
        raise ValueError(f"Could not extract price/area for {url}")

    return RealEstateSchema(
        prid=product_id,
        title=page_title or fallback_title or url,
        url=url,
        city=location["city"],
        district=location["district"],
        ward=location.get("ward"),
        street=location.get("street"),
        full_address=location["full_address"],
        price_total=price_total,
        price_per_m2=price_per_m2,
        price_min_range=price_min,
        price_max_range=price_max,
        area=area,
        frontage=frontage,
        road_width=road_width,
        bedrooms=bedrooms,
        bathrooms=bathrooms,
        floors=floors,
        direction=direction,
        interior=interior,
        legal_status=legal_status,
        post_rank=post_rank_css or "",
        price_trend_1y=price_trend_css,
        is_verified=is_verified_css or "tin xac thuc" in canonical(text) or "da xac thuc" in canonical(text),
        has_elevator=desc_flags["has_elevator"],
        near_park=desc_flags["near_park"],
        is_frontage_road=desc_flags["is_frontage_road"],
        description=description,
        posted_date=posted_date,
        expiry_date=expiry_date,
    )


def extract_catalog_urls(html: str, base_url: str) -> list[str]:
    soup = BeautifulSoup(html, "html.parser")
    urls: list[str] = []
    seen: set[str] = set()

    for anchor in soup.select("a[href]"):
        href = anchor.get("href") or ""
        absolute = make_absolute_url(base_url, href)
        parsed = urlparse(absolute)
        if parsed.netloc and "batdongsan.com.vn" not in parsed.netloc:
            continue
        if not DETAIL_URL_PATTERN.search(parsed.path + ("?" + parsed.query if parsed.query else "")):
            continue
        if absolute not in seen:
            seen.add(absolute)
            urls.append(absolute)
    return urls


def build_catalog_page_url(start_url: str, page_number: int) -> str:
    parsed = urlparse(start_url)
    if page_number <= 1:
        return start_url

    path = parsed.path
    page_match = re.search(r"/p(\d+)$", path)
    if page_match:
        base_page = int(page_match.group(1))
        target_page = base_page + page_number - 1
        path = re.sub(r"/p\d+$", f"/p{target_page}", path)
    else:
        path = f"{path.rstrip('/')}/p{page_number}"

    return parsed._replace(path=path).geturl()


def fallback_fetch_html(url: str) -> str:
    headers = {"User-Agent": USER_AGENT, "Accept-Language": "vi-VN,vi;q=0.9,en-US;q=0.8,en;q=0.7"}
    request = Request(url, headers=headers)

    with urlopen(request, timeout=45) as response:
        payload = response.read()
        charset = response.headers.get_content_charset() or "utf-8"
        return payload.decode(charset, errors="replace")


async def fallback_fetch_html_async(url: str) -> str:
    return await asyncio.to_thread(fallback_fetch_html, url)


async def crawl_single_page(crawler: AsyncWebCrawler, url: str, config: CrawlerRunConfig) -> tuple[str, str]:
    result = await crawler.arun(url=url, config=config)
    if not getattr(result, "success", False):
        raise RuntimeError(f"Crawl failed for {url}: {getattr(result, 'error_message', 'unknown error')}")

    html = extract_html(result)
    if not html.strip():
        raise RuntimeError(f"No HTML returned for {url}")
    return url, html


async def robust_crawl_single_page(
    crawler: AsyncWebCrawler,
    url: str,
    config: CrawlerRunConfig,
    max_attempts: Optional[int] = None,
    base_backoff: Optional[float] = None,
) -> tuple[str, str]:
    """Wrapper around crawl_single_page with exponential backoff and special handling
    for transient navigation errors and Cloudflare JS challenges.
    """
    max_attempts = max_attempts or CURRENT_MAX_RETRIES
    base_backoff = base_backoff or CURRENT_RETRY_BASE_BACKOFF

    for attempt in range(1, max_attempts + 1):
        try:
            return await crawl_single_page(crawler, url, config)
        except Exception as exc:
            msg = str(exc)
            if attempt == max_attempts:
                raise

            # Treat navigation aborts and Cloudflare challenge as transient and backoff
            if (
                "ERR_ABORTED" in msg
                or "frame was detached" in msg
                or "Cloudflare JS challenge" in msg
                or "Blocked by anti-bot" in msg
                or is_cf_challenge(msg)
            ):
                backoff = base_backoff * (2 ** (attempt - 1)) + random.uniform(0, 1)
                logging.info("Transient error for %s: %s — retrying in %.1fs (attempt %d/%d)", url, msg, backoff, attempt, max_attempts)
                await asyncio.sleep(backoff)
                continue
            # Other errors: short wait then retry once
            logging.info("Error fetching %s: %s — retrying shortly (attempt %d/%d)", url, msg, attempt, max_attempts)
            await asyncio.sleep(1 + random.random())


async def navigate_page(page, url: str, timeout_ms: int = DEFAULT_TIMEOUT_MS) -> str:
    """Navigate an existing Playwright page to *url* and return its HTML.
    Handles Cloudflare challenge redirects gracefully by polling the page title
    instead of using evaluate() which can crash during mid-navigation.
    """
    try:
        goto = getattr(page, "goto", None)
        if callable(goto):
            maybe = goto(url, wait_until="domcontentloaded", timeout=timeout_ms)
            if inspect.isawaitable(maybe):
                await maybe
    except Exception as exc:
        logging.info("navigate_page goto failed for %s: %s", url, exc)

    # Wait for any CF challenge redirect to complete by polling page title.
    # During CF challenge, the title is "Just a moment..." — once it changes,
    # the real page has loaded.
    for i in range(30):
        try:
            title = await page.title()
            title_str = str(title or "").lower()
            if title_str and "just a moment" not in title_str:
                break
        except Exception:
            pass  # Context may be destroyed during navigation — just wait
        await asyncio.sleep(1)

    # Wait for load state to ensure page is fully loaded
    try:
        wait_for_load = getattr(page, "wait_for_load_state", None)
        if callable(wait_for_load):
            maybe = wait_for_load("domcontentloaded", timeout=15000)
            if inspect.isawaitable(maybe):
                await maybe
    except Exception:
        pass

    # Small extra wait for dynamic content
    await asyncio.sleep(random.uniform(1.0, 2.0))

    # Extract HTML from page
    content_call = getattr(page, "content", None)
    if callable(content_call):
        html = content_call()
        if inspect.isawaitable(html):
            html = await html
        return html or ""
    return ""


async def perform_cf_warmup(
    crawler: AsyncWebCrawler,
    seed_urls: list[str],
    attempts: int = 3,
    *,
    headful: bool = False,
    headful_wait: int = 0,
) -> tuple[dict[str, str], Any]:
    """Try additional warm-up navigations to obtain Cloudflare clearance cookies.
    When running headful, opens a page in the browser context and polls for CF.
    Returns (cookie_map, warm_page) — the page is kept open for reuse.
    """
    last_cookies: dict[str, str] = {}

    for attempt in range(1, attempts + 1):
        logging.info("CF warm-up attempt %d/%d", attempt, attempts)
        for url in seed_urls:
            try:
                context = _find_browser_context(crawler)
                if context is None or not hasattr(context, "new_page"):
                    logging.info("Browser context not available for interactive warm-up")
                    # Fallback: try crawl4ai arun (may fail but can set cookie)
                    try:
                        await robust_crawl_single_page(crawler, url, build_run_config())
                    except Exception:
                        pass
                    cookies = await extract_session_cookies(crawler)
                    if cookies.get("cf_clearance"):
                        logging.info("cf_clearance acquired on warm-up")
                        return cookies, None
                    last_cookies = cookies
                    continue

                # Interactive warm-up: open a page in the browser context
                page = context.new_page()
                if inspect.isawaitable(page):
                    page = await page

                try:
                    goto = getattr(page, "goto", None)
                    if callable(goto):
                        try:
                            maybe = goto(url, wait_until="domcontentloaded", timeout=DEFAULT_TIMEOUT_MS)
                            if inspect.isawaitable(maybe):
                                await maybe
                        except Exception as exc:
                            logging.info("Interactive warm-up goto failed for %s: %s", url, exc)

                    # Wait for CF challenge to resolve by polling page title
                    poll_timeout = max(30, min(headful_wait, 120)) if headful_wait else 30
                    poll_elapsed = 0.0
                    while poll_elapsed < poll_timeout:
                        try:
                            title = await page.title()
                            title_str = str(title or "").lower()
                            if title_str and "just a moment" not in title_str:
                                logging.info("CF challenge resolved after %.1fs for %s (title: %s)", poll_elapsed, url, title)
                                break
                        except Exception:
                            pass  # Context may be destroyed during CF redirect
                        await asyncio.sleep(1)
                        poll_elapsed += 1

                    # Additional wait
                    if headful_wait and headful_wait > 0:
                        await asyncio.sleep(min(5, headful_wait))

                    # Gentle user interaction
                    try:
                        eval_call = getattr(page, "evaluate", None)
                        if callable(eval_call):
                            await eval_call("() => {window.scrollTo(0, document.body.scrollHeight);}")
                            await asyncio.sleep(random.uniform(0.5, 1.5))
                            await eval_call("() => {window.scrollTo(0, 0);}")
                            await asyncio.sleep(random.uniform(0.5, 1.0))
                    except Exception:
                        pass

                    await asyncio.sleep(2 + random.uniform(1.0, 3.0))

                    cookies = await extract_session_cookies(crawler)
                    if cookies.get("cf_clearance"):
                        logging.info("cf_clearance acquired on interactive warm-up for %s", url)
                        # Return the page — caller can reuse it
                        return cookies, page

                    # Check if the page content is actually resolved (CF might not set cookie but page works)
                    content_call = getattr(page, "content", None)
                    if callable(content_call):
                        html = content_call()
                        if inspect.isawaitable(html):
                            html = await html
                        if html and not is_cf_challenge(html):
                            logging.info("Page content resolved (no CF challenge) for %s — keeping page", url)
                            return cookies, page

                    last_cookies = cookies
                    # Close page since challenge wasn't solved — will try again
                    try:
                        close_call = getattr(page, "close", None)
                        if callable(close_call):
                            maybe = close_call()
                            if inspect.isawaitable(maybe):
                                await maybe
                    except Exception:
                        pass

                except Exception:
                    # On error, close the page
                    try:
                        close_call = getattr(page, "close", None)
                        if callable(close_call):
                            maybe = close_call()
                            if inspect.isawaitable(maybe):
                                await maybe
                    except Exception:
                        pass
                    raise

            except Exception as exc:
                logging.info("Warm-up navigation failed for %s: %s", url, exc)

        # backoff between warm-up rounds
        await asyncio.sleep(2 ** attempt + random.uniform(0, 2))

    return last_cookies, None



async def collect_catalog_links_via_page(page, start_url: str, max_pages: int) -> list[str]:
    """Collect detail URLs from catalog pages using a persistent Playwright page."""
    collected: list[str] = []
    seen: set[str] = set()

    for page_number in range(1, max_pages + 1):
        if STOP_REQUESTED:
            logging.info("Stop requested during catalog collection. Ending early.")
            break
        page_url = build_catalog_page_url(start_url, page_number)
        logging.info("Catalog page %s: %s", page_number, page_url)
        html = ""
        for attempt in range(1, 4):
            try:
                if page_number > 1 or attempt > 1:
                    wait_sec = random.uniform(3.0, 6.0)
                    logging.info("Waiting %.1fs before catalog page %s...", wait_sec, page_url)
                    await asyncio.sleep(wait_sec)

                html = await navigate_page(page, page_url)
                if html.strip() and not is_cf_challenge(html):
                    logging.info("Catalog page %s fetched successfully (%d bytes)", page_number, len(html))
                    break
                elif is_cf_challenge(html):
                    logging.warning("Catalog page %s returned CF challenge; retrying...", page_url)
                    html = ""
                    await asyncio.sleep(3 + random.uniform(1, 3))
            except Exception as exc:
                if attempt == 3:
                    logging.warning("Skipping catalog page %s after 3 attempts: %s", page_url, exc)
                else:
                    logging.info("Retry %s for catalog page %s: %s", attempt, page_url, exc)
                    await asyncio.sleep(2 * attempt)

        if not html:
            continue

        for detail_url in extract_catalog_urls(html, page_url):
            if detail_url not in seen:
                seen.add(detail_url)
                collected.append(detail_url)

    return collected


async def collect_catalog_links(crawler: AsyncWebCrawler, start_url: str, max_pages: int, config: CrawlerRunConfig) -> list[str]:
    """Fallback: collect catalog links using crawl4ai's arun() method."""
    collected: list[str] = []
    seen: set[str] = set()

    for page_number in range(1, max_pages + 1):
        if STOP_REQUESTED:
            logging.info("Stop requested during catalog collection. Ending early.")
            break
        page_url = build_catalog_page_url(start_url, page_number)
        logging.info("Catalog page %s: %s", page_number, page_url)
        html = ""
        for attempt in range(1, 4):
            try:
                if page_number > 1 or attempt > 1:
                    wait_sec = random.uniform(5.0, 10.0)
                    logging.info("Waiting %.1fs before crawling %s...", wait_sec, page_url)
                    await asyncio.sleep(wait_sec)

                _, html = await robust_crawl_single_page(crawler, page_url, config)
                if html.strip() and not is_cf_challenge(html):
                    break
                elif is_cf_challenge(html):
                    logging.warning("Catalog page %s returned CF challenge HTML; retrying...", page_url)
                    html = ""
                    await asyncio.sleep(3 + random.uniform(1, 3))
            except Exception as exc:
                if attempt == 3:
                    logging.warning("Skipping catalog page %s after 3 attempts: %s", page_url, exc)
                else:
                    logging.info("Retry %s for catalog page %s due to: %s", attempt, page_url, exc)
                    await asyncio.sleep(2 * attempt)

        if not html:
            continue

        for detail_url in extract_catalog_urls(html, page_url):
            if detail_url not in seen:
                seen.add(detail_url)
                collected.append(detail_url)

    return collected





async def crawl_details_browser(
    crawler: AsyncWebCrawler,
    urls: list[str],
    config_template: CrawlerRunConfig,
    delay_min: float,
    delay_max: float,
    max_concurrent: int,
    orchestrator: Optional[CrawlOrchestrator] = None,
    stop_checker: Optional[Callable[[], bool]] = None,
) -> tuple[list[RealEstateSchema], list[str]]:
    """Crawl detail pages using a persistent browser context."""
    records: list[RealEstateSchema] = []
    failed: list[str] = []
    records_lock = asyncio.Lock()
    semaphore = asyncio.Semaphore(max_concurrent)

    async def crawl_one_detail(index: int, url: str) -> None:
        async with semaphore:
            if stop_checker and stop_checker():
                return
            await asyncio.sleep(random.uniform(0, 0.5))
            for attempt in range(1, 4):
                if stop_checker and stop_checker():
                    break
                attempt_str = f" [Attempt {attempt}/3]" if attempt > 1 else ""
                logging.info("Detail %s/%s%s: %s", index + 1, len(urls), attempt_str, url)
                try:
                    _, html = await robust_crawl_single_page(crawler, url, config_template)
                    record = parse_detail_page(url, html)
                    if orchestrator is not None:
                        await orchestrator.record_success(record)
                    else:
                        async with records_lock:
                            records.append(record)
                    break
                except (ValidationError, ValueError) as exc:
                    logging.warning("Skipping detail page %s (Data Error): %s", url, exc)
                    break
                except Exception as exc:
                    if attempt == 3:
                        logging.warning("Failed detail page %s after 3 attempts: %s", url, exc)
                        async with records_lock:
                            failed.append(url)
                    else:
                        wait_time = delay_min * attempt * 2
                        logging.info("Retrying %s in %.1fs due to network error: %s", url, wait_time, exc)
                        await asyncio.sleep(wait_time)

            await asyncio.sleep(random.uniform(delay_min, delay_max))

    tasks = [crawl_one_detail(index, url) for index, url in enumerate(urls)]
    await asyncio.gather(*tasks, return_exceptions=False)
    return records, failed


async def crawl_details_httpx(
    urls: list[str],
    cookies: dict[str, str],
    user_agent: str,
    max_concurrent: int,
    proxy_pool: list[str],
    orchestrator: Optional[CrawlOrchestrator] = None,
    stop_checker: Optional[Callable[[], bool]] = None,
) -> tuple[list[RealEstateSchema], list[str]]:
    """Crawl detail pages using httpx with cf_clearance cookies for speed."""
    records: list[RealEstateSchema] = []
    failed: list[str] = []
    records_lock = asyncio.Lock()
    semaphore = asyncio.Semaphore(max_concurrent)
    proxy_cycle = itertools.cycle(proxy_pool) if proxy_pool else None
    proxy_lock = asyncio.Lock()

    headers = build_httpx_headers(user_agent)
    timeout = httpx.Timeout(DEFAULT_HTTPX_TIMEOUT)
    proxy_clients: dict[str, httpx.AsyncClient] = {}
    client_no_proxy = httpx.AsyncClient(
        headers=headers,
        cookies=cookies,
        timeout=timeout,
        follow_redirects=True,
    )

    for proxy in proxy_pool:
        if proxy in proxy_clients:
            continue
        proxy_clients[proxy] = httpx.AsyncClient(
            headers=headers,
            cookies=cookies,
            timeout=timeout,
            follow_redirects=True,
            proxies=build_proxy_config_dict(proxy),
        )

    try:
        async def fetch_one(index: int, url: str) -> None:
            async with semaphore:
                if stop_checker and stop_checker():
                    return
                client = client_no_proxy
                if proxy_cycle is not None:
                    async with proxy_lock:
                        proxy = next(proxy_cycle)
                    client = proxy_clients.get(proxy, client_no_proxy)
                try:
                    response = await client.get(url)
                    if response.status_code >= 400 or is_cf_challenge(response.text):
                        raise RuntimeError(f"Blocked or challenge: {response.status_code}")
                    record = parse_detail_page(url, response.text)
                    if orchestrator is not None:
                        await orchestrator.record_success(record)
                    else:
                        async with records_lock:
                            records.append(record)
                except (ValidationError, ValueError) as exc:
                    logging.warning("Skipping detail page %s (Data Error): %s", url, exc)
                except Exception as exc:
                    logging.warning("HTTPX failed for %s: %s", url, exc)
                    async with records_lock:
                        failed.append(url)

        tasks = [fetch_one(index, url) for index, url in enumerate(urls)]
        await asyncio.gather(*tasks, return_exceptions=False)
    finally:
        await client_no_proxy.aclose()
        for proxy_client in proxy_clients.values():
            await proxy_client.aclose()

    return records, failed


def write_csv(records: list[RealEstateSchema], output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = list(RealEstateSchema.model_fields.keys())

    with output_path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()

        for record in records:
            data = record.model_dump()
            data["posted_date"] = data["posted_date"].isoformat()
            data["expiry_date"] = data["expiry_date"].isoformat()
            writer.writerow(data)


def append_csv(records: list[RealEstateSchema], output_path: Path) -> None:
    if not records:
        return
    output_path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = list(RealEstateSchema.model_fields.keys())
    write_header = (not output_path.exists()) or output_path.stat().st_size == 0

    with output_path.open("a", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        if write_header:
            writer.writeheader()
        for record in records:
            data = record.model_dump()
            data["posted_date"] = data["posted_date"].isoformat()
            data["expiry_date"] = data["expiry_date"].isoformat()
            writer.writerow(data)


def _handle_stop_signal(signum, _frame) -> None:
    global STOP_REQUESTED
    if STOP_REQUESTED:
        return
    STOP_REQUESTED = True
    logging.warning("Received stop signal %s. Will stop after current in-flight work.", signum)


def register_signal_handlers() -> None:
    for sig_name in ("SIGINT", "SIGTERM"):
        sig = getattr(signal, sig_name, None)
        if sig is None:
            continue
        try:
            signal.signal(sig, _handle_stop_signal)
        except Exception:
            # Some environments may not allow replacing handlers.
            continue


class CrawlOrchestrator:
    def __init__(self, output_path: Path, progress_path: Path, batch_size: int) -> None:
        self.output_path = output_path
        self.progress_path = progress_path
        self.batch_size = max(1, batch_size)
        self._lock = asyncio.Lock()
        self._buffer: list[RealEstateSchema] = []
        self._processed_urls: set[str] = set()
        self._processed_ids: set[str] = set()
        self.new_success_count = 0
        self._load_progress()

    @property
    def processed_count(self) -> int:
        return len(self._processed_urls)

    def stop_requested(self) -> bool:
        return STOP_REQUESTED

    def _load_progress(self) -> None:
        if not self.progress_path.exists():
            return
        try:
            payload = json.loads(self.progress_path.read_text(encoding="utf-8"))
        except Exception as exc:
            logging.warning("Cannot load progress file %s: %s", self.progress_path, exc)
            return

        self._processed_urls = set(payload.get("crawled_urls") or [])
        self._processed_ids = set(payload.get("crawled_ids") or [])

    def _save_progress(self) -> None:
        payload = {
            "version": 1,
            "updated_at": datetime.utcnow().isoformat() + "Z",
            "crawled_urls": sorted(self._processed_urls),
            "crawled_ids": sorted(self._processed_ids),
            "success_count": len(self._processed_urls),
        }
        self.progress_path.parent.mkdir(parents=True, exist_ok=True)
        tmp_path = self.progress_path.with_suffix(self.progress_path.suffix + ".tmp")
        tmp_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        tmp_path.replace(self.progress_path)

    def filter_pending_urls(self, urls: list[str]) -> list[str]:
        return [url for url in urls if url not in self._processed_urls]

    async def record_success(self, record: RealEstateSchema) -> None:
        async with self._lock:
            if record.url in self._processed_urls:
                return
            self._processed_urls.add(record.url)
            self._processed_ids.add(record.product_id)
            self._buffer.append(record)
            self.new_success_count += 1
            self._save_progress()

            if len(self._buffer) >= self.batch_size:
                self._flush_buffer_locked()

    def _flush_buffer_locked(self) -> None:
        if not self._buffer:
            return
        append_csv(self._buffer, self.output_path)
        logging.info("Autosaved %d records to %s", len(self._buffer), self.output_path)
        self._buffer.clear()

    async def flush(self) -> None:
        async with self._lock:
            self._flush_buffer_locked()


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Crawl batdongsan.com.vn listings into CSV with parallel processing")
    parser.add_argument("--start-url", default=DEFAULT_START_URL, help="Catalog URL to start from")
    parser.add_argument("--max-pages", type=int, default=DEFAULT_MAX_PAGES, help="Number of catalog pages to crawl")
    parser.add_argument("--output", default=DEFAULT_OUTPUT, help="CSV output file path")
    parser.add_argument("--delay-min", type=float, default=DEFAULT_DELAY_MIN, help="Minimum delay between detail requests (seconds)")
    parser.add_argument("--delay-max", type=float, default=DEFAULT_DELAY_MAX, help="Maximum delay between detail requests (seconds)")
    parser.add_argument("--concurrency", type=int, default=DEFAULT_CONCURRENCY, help="Max concurrent requests in scale phase (1-10 recommended)")
    parser.add_argument("--warmup-count", type=int, default=DEFAULT_WARMUP_COUNT, help="Number of warm-up URLs to fetch sequentially")
    parser.add_argument("--user-data-dir", default=DEFAULT_USER_DATA_DIR, help="Persistent browser profile directory")
    parser.add_argument("--proxy-file", default=DEFAULT_PROXY_FILE, help="Proxy list file (one proxy per line)")
    parser.add_argument("--use-proxy", action="store_true", help="Enable proxy rotation for HTTPX stage")
    parser.add_argument("--timeout-ms", type=int, default=DEFAULT_TIMEOUT_MS, help="Navigation timeout in milliseconds")
    parser.add_argument("--headful", action="store_true", help="Run browser in headful (non-headless) mode for better stealth")
    parser.add_argument(
        "--headful-wait",
        type=int,
        default=30,
        help="Seconds to wait after headful warm-up to allow manual Cloudflare interaction",
    )
    parser.add_argument("--max-retries", type=int, default=DEFAULT_MAX_RETRIES, help="Max retries for internal crawl attempts")
    parser.add_argument("--retry-backoff", type=float, default=DEFAULT_RETRY_BASE_BACKOFF, help="Base backoff seconds for retries")
    parser.add_argument("--progress-file", default=DEFAULT_PROGRESS_FILE, help="Progress checkpoint JSON path")
    parser.add_argument(
        "--autosave-batch-size",
        type=int,
        default=DEFAULT_AUTOSAVE_BATCH_SIZE,
        help="Append to CSV after this many successful records",
    )
    parser.add_argument("--log-level", default="INFO", help="Logging level (DEBUG, INFO, WARNING, ERROR)")
    return parser.parse_args()


async def main_async() -> int:
    args = parse_args()
    logging.basicConfig(
        level=getattr(logging, args.log_level.upper(), logging.INFO),
        format="%(asctime)s [%(levelname)s] %(message)s"
    )
    register_signal_handlers()

    concurrency = max(1, min(args.concurrency, 10))
    if concurrency != args.concurrency:
        logging.warning("Concurrency adjusted to %d (must be 1-10)", concurrency)

    warmup_count = max(0, args.warmup_count)
    user_data_dir = Path(args.user_data_dir)
    user_data_dir.mkdir(parents=True, exist_ok=True)
    orchestrator = CrawlOrchestrator(
        output_path=Path(args.output),
        progress_path=Path(args.progress_file),
        batch_size=max(1, int(args.autosave_batch_size)),
    )
    logging.info("Loaded progress: %d URLs already completed", orchestrator.processed_count)

    proxy_pool = load_proxies(Path(args.proxy_file)) if args.use_proxy else []
    if args.use_proxy and not proxy_pool:
        logging.warning("Proxy rotation enabled but no proxies were found in %s", args.proxy_file)

    # Allow runtime override of the default page timeout
    global DEFAULT_TIMEOUT_MS
    try:
        DEFAULT_TIMEOUT_MS = max(1000, int(args.timeout_ms))
    except Exception:
        DEFAULT_TIMEOUT_MS = DEFAULT_TIMEOUT_MS

    browser_proxy = proxy_pool[0] if proxy_pool else None
    # Quick proxy validation: if the selected proxy can't fetch the start URL, fall back to direct
    if browser_proxy:
        valid = await validate_proxy_async(browser_proxy, args.start_url, timeout=min(10.0, DEFAULT_HTTPX_TIMEOUT))
        if not valid:
            logging.warning("Selected proxy %s failed quick validation; falling back to direct connection", browser_proxy)
            browser_proxy = None
    # Apply CLI-configured retry/backoff to the module-level settings
    global CURRENT_MAX_RETRIES, CURRENT_RETRY_BASE_BACKOFF
    CURRENT_MAX_RETRIES = max(1, args.max_retries)
    CURRENT_RETRY_BASE_BACKOFF = max(0.1, args.retry_backoff)

    # Pick a user-agent for this run and build browser config (headful optionally)
    selected_user_agent = random.choice(USER_AGENTS)
    browser_config = build_browser_config(
        user_data_dir=user_data_dir, proxy_url=browser_proxy, headful=args.headful, user_agent=selected_user_agent
    )
    dispatcher = build_dispatcher(DEFAULT_MAX_SESSION_QUERIES)
    warmup_run_config = build_run_config()
    scale_run_config = build_run_config(dispatcher)

    async with AsyncWebCrawler(config=browser_config) as crawler:
        # ── PHASE 0: Cloudflare warm-up BEFORE catalog crawling ──
        # The site is protected by Cloudflare JS challenge; we need to
        # establish a clearance cookie and get a warm page before any real crawling.
        logging.info("Phase 0: Cloudflare warm-up (before catalog crawl)")
        seed_warmup_urls = [
            args.start_url,
            f"{urlparse(args.start_url).scheme}://{urlparse(args.start_url).netloc}",
        ]
        cookies, warm_page = await perform_cf_warmup(
            crawler,
            seed_warmup_urls,
            attempts=3,
            headful=args.headful,
            headful_wait=getattr(args, "headful_wait", 0),
        )
        log_cf_clearance(cookies)

        # If running headful and no clearance yet, give user a chance to solve manually
        if not cookies.get("cf_clearance") and warm_page is None and args.headful:
            logging.info("No cf_clearance found after automated warm-up.")
            logging.info(
                "Switch to the opened browser, complete the Cloudflare challenge, then return and press Enter to continue."
            )
            try:
                await asyncio.to_thread(input, "Press Enter after solving the challenge in the browser...")
            except Exception:
                logging.info("Interactive prompt failed; proceeding anyway")
            cookies = await extract_session_cookies(crawler)
            log_cf_clearance(cookies)

        # ── PHASE 1: Catalog crawling ──
        catalog_urls: list[str] = []
        if warm_page is not None:
            # Use the warm page that already passed CF challenge
            logging.info("Using warm page for catalog crawling (bypasses CF)")
            catalog_urls = await collect_catalog_links_via_page(warm_page, args.start_url, args.max_pages)
        else:
            # Fallback to crawl4ai arun
            logging.info("No warm page available; using crawl4ai for catalog crawling")
            catalog_urls = await collect_catalog_links(crawler, args.start_url, args.max_pages, warmup_run_config)

        if not catalog_urls:
            if orchestrator.stop_requested():
                await orchestrator.flush()
                logging.info("Stopped before detail crawl started.")
                return 130
            logging.error("No detail URLs were found from the catalog pages")
            return 1

        logging.info("Collected %s candidate detail URLs", len(catalog_urls))
        pending_urls = orchestrator.filter_pending_urls(catalog_urls)
        logging.info("Pending detail URLs after resume filter: %d", len(pending_urls))
        if not pending_urls:
            await orchestrator.flush()
            logging.info("No pending URLs. Output is up to date at %s", orchestrator.output_path.resolve())
            return 0

        # ── PHASE 2: Detail page crawling ──
        if warm_page is not None:
            # Use the warm page to crawl detail pages sequentially
            logging.info("Detail phase: using warm page for %d URLs (sequential)", len(pending_urls))
            for index, detail_url in enumerate(pending_urls):
                if orchestrator.stop_requested():
                    logging.info("Stop requested. Exiting detail loop after current URL.")
                    break
                logging.info("Detail %s/%s: %s", index + 1, len(pending_urls), detail_url)
                try:
                    await asyncio.sleep(random.uniform(args.delay_min, args.delay_max))
                    html = await navigate_page(warm_page, detail_url)
                    if is_cf_challenge(html):
                        logging.warning("CF challenge on detail page %s; skipping", detail_url)
                        continue
                    record = parse_detail_page(detail_url, html)
                    await orchestrator.record_success(record)
                except (ValidationError, ValueError) as exc:
                    logging.warning("Skipping detail page %s (Data Error): %s", detail_url, exc)
                except Exception as exc:
                    logging.warning("Failed detail page %s: %s", detail_url, exc)

            # Close the warm page when done
            try:
                close_call = getattr(warm_page, "close", None)
                if callable(close_call):
                    maybe = close_call()
                    if inspect.isawaitable(maybe):
                        await maybe
            except Exception:
                pass
        else:
            # Fallback: use crawl4ai browser crawling
            warmup_urls = pending_urls[:warmup_count]
            remaining_urls = pending_urls[warmup_count:]

            if warmup_urls and not orchestrator.stop_requested():
                logging.info("Warm-up phase: %d URLs with concurrency=1", len(warmup_urls))
                warmup_records, warmup_failed = await crawl_details_browser(
                    crawler,
                    warmup_urls,
                    warmup_run_config,
                    args.delay_min,
                    args.delay_max,
                    max_concurrent=1,
                    orchestrator=orchestrator,
                    stop_checker=orchestrator.stop_requested,
                )
                if warmup_records:
                    logging.debug("Warm-up collected %d records in non-orchestrated mode", len(warmup_records))
                if warmup_failed:
                    logging.warning("Warm-up failed for %d URLs", len(warmup_failed))

            # Refresh cookies after warm-up phase
            cookies = await extract_session_cookies(crawler)
            log_cf_clearance(cookies)

            if remaining_urls and not orchestrator.stop_requested():
                if cookies.get("cf_clearance"):
                    logging.info("Scale phase: HTTPX with concurrency=%d", concurrency)
                    httpx_records, httpx_failed = await crawl_details_httpx(
                        remaining_urls,
                        cookies,
                        selected_user_agent,
                        concurrency,
                        proxy_pool,
                        orchestrator=orchestrator,
                        stop_checker=orchestrator.stop_requested,
                    )
                    if httpx_records:
                        logging.debug("HTTPX collected %d records in non-orchestrated mode", len(httpx_records))

                    if httpx_failed:
                        logging.info("HTTPX fallback to browser for %d URLs", len(httpx_failed))
                        browser_records, browser_failed = await crawl_details_browser(
                            crawler,
                            httpx_failed,
                            scale_run_config,
                            args.delay_min,
                            args.delay_max,
                            max_concurrent=concurrency,
                            orchestrator=orchestrator,
                            stop_checker=orchestrator.stop_requested,
                        )
                        if browser_records:
                            logging.debug("Fallback browser collected %d records in non-orchestrated mode", len(browser_records))
                        if browser_failed:
                            logging.warning("Browser fallback failed for %d URLs", len(browser_failed))
                else:
                    logging.info("Scale phase: browser with concurrency=%d", concurrency)
                    browser_records, browser_failed = await crawl_details_browser(
                        crawler,
                        remaining_urls,
                        scale_run_config,
                        args.delay_min,
                        args.delay_max,
                        max_concurrent=concurrency,
                        orchestrator=orchestrator,
                        stop_checker=orchestrator.stop_requested,
                    )
                    if browser_records:
                        logging.debug("Browser scale collected %d records in non-orchestrated mode", len(browser_records))
                    if browser_failed:
                        logging.warning("Browser scale failed for %d URLs", len(browser_failed))

    await orchestrator.flush()

    if orchestrator.processed_count == 0:
        logging.error("No valid records were extracted")
        return 1

    logging.info(
        "Run completed. New records: %d | Total progress: %d | Output: %s",
        orchestrator.new_success_count,
        orchestrator.processed_count,
        orchestrator.output_path.resolve(),
    )
    return 130 if orchestrator.stop_requested() else 0


def main() -> None:
    raise SystemExit(asyncio.run(main_async()))


if __name__ == "__main__":
    main()