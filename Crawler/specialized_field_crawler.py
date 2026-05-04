from __future__ import annotations

import argparse
import asyncio
import csv
import json
import logging
import inspect
import random
import re
import unicodedata
import signal
from contextlib import suppress
from pathlib import Path
from typing import Any, Optional
from urllib.parse import urlparse

import httpx
from bs4 import BeautifulSoup

DEFAULT_CONCURRENCY = 1
DEFAULT_DELAY_MIN = 0.7
DEFAULT_DELAY_MAX = 1.8
DEFAULT_TIMEOUT_MS = 90_000
DEFAULT_USER_DATA_DIR = ".crawler_profile"
DEFAULT_PROXY_FILE = "proxies.txt"
DEFAULT_HTTPX_TIMEOUT = 25.0
DEFAULT_MAX_RETRIES = 5
DEFAULT_RETRY_BASE_BACKOFF = 2.0
DEFAULT_WARMUP_COUNT = 3

STOP_REQUESTED = False

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.0 Safari/605.1.15",
]


DEFAULT_INPUT_CSV = "Dataset/batdongsan_real_estate_cleaned.csv"
DEFAULT_PROGRESS_FILE = "specialized_field_progress.json"
DEFAULT_AUTOSAVE_BATCH_SIZE = 80

def normalize_text(text: str) -> str:
    return " ".join((text or "").split()).strip()


def strip_accents(text: str) -> str:
    normalized = unicodedata.normalize("NFKD", text)
    return "".join(char for char in normalized if not unicodedata.combining(char))


def canonical(text: str) -> str:
    return strip_accents(normalize_text(text)).lower()


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


def build_httpx_headers(user_agent: str) -> dict[str, str]:
    return {
        "User-Agent": user_agent,
        "Accept-Language": "vi-VN,vi;q=0.9,en-US;q=0.8,en;q=0.7",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Cache-Control": "no-cache",
        "Pragma": "no-cache",
    }


def build_proxy_config_dict(proxy_url: Optional[str]) -> Optional[dict[str, str]]:
    if not proxy_url:
        return None
    return {"all://": proxy_url}


def filter_supported_kwargs(target: Any, kwargs: dict[str, Any]) -> dict[str, Any]:
    try:
        parameters = inspect.signature(target).parameters
    except (TypeError, ValueError):
        return kwargs
    return {key: value for key, value in kwargs.items() if key in parameters}


def is_cf_challenge(html: str) -> bool:
    if not html:
        return True
    html_lower = html.lower()
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
    if len(html) < 15000:
        weak_markers = ["cf-challenge", "attention required"]
        if any(marker in html_lower for marker in weak_markers):
            return True
    return False


def build_playwright_storage_state(cookies: list[dict[str, Any]]) -> dict[str, Any]:
    return {"cookies": cookies, "origins": []}


async def extract_playwright_cookies(context: Any) -> tuple[dict[str, str], list[dict[str, Any]]]:
    cookies_call = getattr(context, "cookies", None)
    if cookies_call is None:
        return {}, []

    cookies = cookies_call()
    if inspect.isawaitable(cookies):
        cookies = await cookies

    cookie_map: dict[str, str] = {}
    cookie_list: list[dict[str, Any]] = []
    for cookie in cookies or []:
        if not isinstance(cookie, dict):
            continue
        cookie_list.append(cookie)
        name = cookie.get("name")
        value = cookie.get("value")
        if name and value:
            cookie_map[str(name)] = str(value)
    return cookie_map, cookie_list


async def save_profile_snapshot(context: Any, user_data_dir: Optional[Path]) -> Optional[Path]:
    if user_data_dir is None:
        return None

    snapshot_path = user_data_dir / "storage_state.json"
    storage_state_call = getattr(context, "storage_state", None)
    if storage_state_call is None:
        return None

    try:
        maybe = storage_state_call(path=str(snapshot_path))
        if inspect.isawaitable(maybe):
            await maybe
        return snapshot_path
    except Exception as exc:
        logging.warning("Could not save profile snapshot to %s: %s", snapshot_path, exc)
        return None


async def wait_for_real_page_html(page: Any, timeout_seconds: float = 20.0) -> str:
    deadline = asyncio.get_running_loop().time() + max(1.0, timeout_seconds)
    last_html = ""

    while True:
        try:
            await page.evaluate("() => { window.scrollTo(0, document.body.scrollHeight); }")
            await asyncio.sleep(0.5)
            await page.evaluate("() => { window.scrollTo(0, 0); }")
        except Exception:
            pass

        html = ""
        try:
            html = await page.content()
        except Exception:
            html = last_html

        if html:
            last_html = html
            if not is_cf_challenge(html):
                return html

        try:
            title = (await page.title() or "").lower()
        except Exception:
            title = ""

        if title and "just a moment" not in title and last_html and not is_cf_challenge(last_html):
            return last_html

        if asyncio.get_running_loop().time() >= deadline:
            return last_html

        await asyncio.sleep(1)


async def warm_up_playwright_session(
    urls: list[str],
    user_agent: str,
    proxy_url: Optional[str],
    user_data_dir: Optional[Path],
    *,
    headful: bool,
    headful_wait: int,
) -> tuple[dict[str, str], list[dict[str, Any]]]:
    try:
        from playwright.async_api import async_playwright
    except Exception as exc:
        logging.warning("Playwright is unavailable for warm-up: %s", exc)
        return {}, []

    launch_kwargs: dict[str, Any] = {"headless": not headful}
    extra_args = ["--disable-blink-features=AutomationControlled"]
    if not headful:
        extra_args.append("--headless=new")
    launch_kwargs["args"] = extra_args
    if headful:
        launch_kwargs["channel"] = "chrome"
    if proxy_url:
        launch_kwargs["proxy"] = {"server": proxy_url}

    async with async_playwright() as playwright:
        try:
            if user_data_dir is not None:
                context = await playwright.chromium.launch_persistent_context(
                    str(user_data_dir),
                    user_agent=user_agent,
                    viewport={"width": 1365, "height": 900},
                    ignore_https_errors=True,
                    **launch_kwargs,
                )
                browser = None
            else:
                browser = await playwright.chromium.launch(**launch_kwargs)
                context = await browser.new_context(
                    user_agent=user_agent,
                    viewport={"width": 1365, "height": 900},
                    ignore_https_errors=True,
                )
        except Exception:
            if headful and "channel" in launch_kwargs:
                logging.warning("Chrome channel launch failed; retrying with Playwright's default Chromium")
                launch_kwargs.pop("channel", None)
                if user_data_dir is not None:
                    context = await playwright.chromium.launch_persistent_context(
                        str(user_data_dir),
                        user_agent=user_agent,
                        viewport={"width": 1365, "height": 900},
                        ignore_https_errors=True,
                        **launch_kwargs,
                    )
                    browser = None
                else:
                    browser = await playwright.chromium.launch(**launch_kwargs)
                    context = await browser.new_context(
                        user_agent=user_agent,
                        viewport={"width": 1365, "height": 900},
                        ignore_https_errors=True,
                    )
            else:
                raise
        page = await context.new_page()

        try:
            for index, url in enumerate(urls[:DEFAULT_WARMUP_COUNT]):
                logging.info("Warm-up %s/%s: %s", index + 1, min(len(urls), DEFAULT_WARMUP_COUNT), url)
                try:
                    await page.goto(url, wait_until="domcontentloaded", timeout=DEFAULT_TIMEOUT_MS)
                    html = await wait_for_real_page_html(page, timeout_seconds=max(10.0, float(headful_wait or 0) + 10.0))
                    if html and not is_cf_challenge(html):
                        break
                except Exception as exc:
                    logging.info("Warm-up navigation failed for %s: %s", url, exc)

            cookie_map, cookie_list = await extract_playwright_cookies(context)
            snapshot_path = await save_profile_snapshot(context, user_data_dir)
            if snapshot_path is not None:
                logging.info("Saved crawler profile snapshot to %s", snapshot_path)
            return cookie_map, cookie_list
        finally:
            with suppress(Exception):
                await page.close()
            with suppress(Exception):
                await context.close()
            if browser is not None:
                with suppress(Exception):
                    await browser.close()


def clean_numeric(text: str) -> Optional[float]:
    if not text:
        return None
    text = re.sub(r"[~\s]+", "", text).strip()
    match = re.search(r"-?[\d.,]+", text)
    if not match:
        return None
    value = match.group(0)
    if "," in value and "." in value:
        if value.rfind(",") > value.rfind("."):
            value = value.replace(".", "").replace(",", ".")
        else:
            value = value.replace(",", "")
    elif "," in value:
        value = value.replace(",", ".")
    try:
        return float(value)
    except ValueError:
        return None


def safe_decimal(raw: str) -> Optional[float]:
    return clean_numeric(raw)


def extract_specialized_field(html_content: str) -> dict[str, Optional[float]]:
    """
    Trích xuất duy nhất 2 trường:
    - Mặt tiền -> frontage
    - Đường vào -> road_width

    Chiến lược:
    - Chỉ đọc trong block thông số `.re__pr-specs-content-item`
    - Chỉ map khi title khớp chính xác với nhãn chuẩn hóa
    - Nếu không có trường thì giữ None, tuyệt đối không fallback sang text nhiễu
    """
    soup = BeautifulSoup(html_content, "html.parser")
    extracted: dict[str, Optional[float]] = {"frontage": None, "road_width": None}

    item_selector = (
        ".re__pr-other-info-display .re__pr-specs-content-item, "
        ".re__pr-specs-content-v2 .re__pr-specs-content-item, "
        ".re__pr-specs-content-item"
    )
    target_map = {
        canonical("Mặt tiền"): "frontage",
        canonical("Đường vào"): "road_width",
    }

    for item in soup.select(item_selector):
        title_elem = item.select_one(".re__pr-specs-content-item-title")
        value_elem = item.select_one(".re__pr-specs-content-item-value")
        if title_elem is None or value_elem is None:
            continue

        title_key = canonical(normalize_text(title_elem.get_text()))
        target_field = target_map.get(title_key)
        if target_field is None:
            continue

        raw_value = normalize_text(value_elem.get_text())
        lower_val = raw_value.lower()
        
        # Prevent picking up price/area info mistakenly tagged as frontage/road_width
        if "triệu" in lower_val or "tỷ" in lower_val or "m²" in lower_val or "m2" in lower_val:
            continue

        numeric_value = safe_decimal(raw_value)
        if extracted[target_field] is None:
            extracted[target_field] = numeric_value

    return extracted


def load_csv_rows(csv_path: Path) -> tuple[list[dict[str, Any]], list[str]]:
    with csv_path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        rows = list(reader)
        fieldnames = list(reader.fieldnames or [])
    return rows, fieldnames


def write_csv_rows(csv_path: Path, rows: list[dict[str, Any]], fieldnames: list[str]) -> None:
    csv_path.parent.mkdir(parents=True, exist_ok=True)
    with csv_path.open("w", encoding="utf-8-sig", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def ensure_output_columns(fieldnames: list[str]) -> list[str]:
    updated = list(fieldnames)
    for column in ("frontage", "road_width"):
        if column not in updated:
            updated.append(column)
    return updated


def value_is_present(value: Any) -> bool:
    if value is None:
        return False
    text = str(value).strip()
    return bool(text) and text.lower() != "nan"


class SpecializedCsvOrchestrator:
    def __init__(self, csv_path: Path, progress_path: Path, batch_size: int, overwrite: bool) -> None:
        self.csv_path = csv_path
        self.progress_path = progress_path
        self.batch_size = max(1, batch_size)
        self.overwrite = overwrite
        self.rows, self.fieldnames = load_csv_rows(csv_path)
        self.fieldnames = ensure_output_columns(self.fieldnames)
        self.row_locks = [asyncio.Lock() for _ in self.rows]
        self.file_lock = asyncio.Lock()
        self.completed_keys: set[str] = set()
        self.updated_since_flush = 0
        self._load_progress()

    def _load_progress(self) -> None:
        if not self.progress_path.exists():
            return
        try:
            payload = json.loads(self.progress_path.read_text(encoding="utf-8"))
        except Exception as exc:
            logging.warning("Cannot load progress file %s: %s", self.progress_path, exc)
            return

        completed = payload.get("completed_keys", [])
        if isinstance(completed, list):
            self.completed_keys = {str(item) for item in completed if str(item).strip()}

    def _save_progress(self) -> None:
        payload = {"completed_keys": sorted(self.completed_keys)}
        self.progress_path.parent.mkdir(parents=True, exist_ok=True)
        tmp_path = self.progress_path.with_suffix(self.progress_path.suffix + ".tmp")
        tmp_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
        tmp_path.replace(self.progress_path)

    def build_jobs(self) -> list[tuple[int, str, str]]:
        jobs: list[tuple[int, str, str]] = []
        for index, row in enumerate(self.rows):
            url = normalize_text(row.get("url", ""))
            if not url:
                continue

            # Use URL as the stable checkpoint key as it is more reliable for this dataset.
            row_key = url

            if self.overwrite:
                jobs.append((index, row_key, url))
                continue

            has_frontage = value_is_present(row.get("frontage"))
            has_road_width = value_is_present(row.get("road_width"))
            has_both_specialized_fields = has_frontage and has_road_width

            if has_both_specialized_fields:
                self.completed_keys.add(row_key)
                continue

            if row_key in self.completed_keys:
                continue

            jobs.append((index, row_key, url))
        return jobs

    async def record_result(self, row_index: int, row_key: str, extracted: dict[str, Optional[float]]) -> None:
        async with self.row_locks[row_index]:
            row = self.rows[row_index]
            row["frontage"] = "" if extracted["frontage"] is None else extracted["frontage"]
            row["road_width"] = "" if extracted["road_width"] is None else extracted["road_width"]
            self.completed_keys.add(row_key)
            self.updated_since_flush += 1

        if self.updated_since_flush >= self.batch_size:
            await self.flush()

    async def mark_failed(self, row_key: str) -> None:
        # Không đánh dấu complete để lần chạy sau có thể retry lại URL lỗi.
        self._save_progress()

    async def flush(self) -> None:
        async with self.file_lock:
            write_csv_rows(self.csv_path, self.rows, self.fieldnames)
            self._save_progress()
            if self.updated_since_flush:
                logging.info("Autosaved %d updated rows to %s", self.updated_since_flush, self.csv_path.resolve())
            self.updated_since_flush = 0


async def fetch_html_via_httpx(
    url: str,
    client: httpx.AsyncClient,
    *,
    max_attempts: int,
    base_backoff: float,
) -> str:
    for attempt in range(1, max_attempts + 1):
        try:
            response = await client.get(url)
            if response.status_code >= 400:
                raise RuntimeError(f"HTTP {response.status_code}")
            html = response.text
            if is_cf_challenge(html):
                raise RuntimeError("Blocked by anti-bot")
            return html
        except Exception:
            if attempt == max_attempts:
                raise
            backoff = base_backoff * (2 ** (attempt - 1)) + random.uniform(0, 1)
            await asyncio.sleep(backoff)
    raise RuntimeError(f"Failed to fetch {url}")


async def crawl_url_batch_httpx(
    jobs: list[tuple[int, str, str]],
    cookies: dict[str, str],
    user_agent: str,
    max_concurrent: int,
    proxy_pool: list[str],
    orchestrator: SpecializedCsvOrchestrator,
) -> list[tuple[int, str, str]]:
    failed: list[tuple[int, str, str]] = []
    semaphore = asyncio.Semaphore(max_concurrent)
    headers = build_httpx_headers(user_agent)
    timeout = httpx.Timeout(DEFAULT_HTTPX_TIMEOUT)

    async def get_client(proxy_url: Optional[str]) -> httpx.AsyncClient:
        client_kwargs = {
            "headers": headers,
            "cookies": cookies,
            "timeout": timeout,
            "follow_redirects": True,
        }
        if proxy_url:
            client_kwargs["proxies"] = build_proxy_config_dict(proxy_url)
        return httpx.AsyncClient(**filter_supported_kwargs(httpx.AsyncClient.__init__, client_kwargs))

    clients: list[httpx.AsyncClient] = []
    base_client = await get_client(None)
    clients.append(base_client)

    async def fetch_one(index: int, row_index: int, row_key: str, url: str) -> None:
        async with semaphore:
            if STOP_REQUESTED:
                return
            client = base_client
            if proxy_pool:
                proxy_url = proxy_pool[index % len(proxy_pool)]
                client = await get_client(proxy_url)
                clients.append(client)
            try:
                html = await fetch_html_via_httpx(
                    url,
                    client,
                    max_attempts=DEFAULT_MAX_RETRIES,
                    base_backoff=DEFAULT_RETRY_BASE_BACKOFF,
                )
                extracted = extract_specialized_field(html)
                await orchestrator.record_result(row_index, row_key, extracted)
                logging.info(
                    "Updated %s | frontage=%s | road_width=%s",
                    url,
                    extracted["frontage"],
                    extracted["road_width"],
                )
            except Exception as exc:
                logging.warning("HTTPX failed for %s: %s", url, exc)
                failed.append((row_index, row_key, url))
            finally:
                await asyncio.sleep(random.uniform(DEFAULT_DELAY_MIN, DEFAULT_DELAY_MAX))

    try:
        tasks = [fetch_one(index, row_index, row_key, url) for index, (row_index, row_key, url) in enumerate(jobs)]
        await asyncio.gather(*tasks, return_exceptions=False)
    finally:
        for client in clients:
            try:
                await client.aclose()
            except Exception:
                pass

    return failed


async def crawl_url_batch_browser(
    jobs: list[tuple[int, str, str]],
    user_agent: str,
    max_concurrent: int,
    proxy_pool: list[str],
    orchestrator: SpecializedCsvOrchestrator,
    headful_wait: int,
    headful: bool,
    user_data_dir: Optional[Path] = None,
    storage_state: Optional[dict[str, Any]] = None,
) -> list[tuple[int, str, str]]:
    try:
        from playwright.async_api import async_playwright
    except Exception as exc:
        raise RuntimeError(
            "Playwright is required for headful crawling, but it is not available in this environment"
        ) from exc

    failed: list[tuple[int, str, str]] = []
    launch_proxy = proxy_pool[0] if proxy_pool else None
    launch_kwargs: dict[str, Any] = {"headless": not headful}
    extra_args = ["--disable-blink-features=AutomationControlled"]
    if not headful:
        extra_args.append("--headless=new")
    launch_kwargs["args"] = extra_args
    if headful:
        launch_kwargs["channel"] = "chrome"
    if launch_proxy:
        launch_kwargs["proxy"] = {"server": launch_proxy}

    async with async_playwright() as playwright:
        browser = None
        try:
            if user_data_dir is not None:
                context = await playwright.chromium.launch_persistent_context(
                    str(user_data_dir),
                    user_agent=user_agent,
                    viewport={"width": 1365, "height": 900},
                    ignore_https_errors=True,
                    **launch_kwargs,
                )
            else:
                browser = await playwright.chromium.launch(**launch_kwargs)
                context = await browser.new_context(
                    user_agent=user_agent,
                    viewport={"width": 1365, "height": 900},
                    ignore_https_errors=True,
                    storage_state=storage_state,
                )
        except Exception:
            if headful and "channel" in launch_kwargs:
                logging.warning("Chrome channel launch failed; retrying with Playwright's default Chromium")
                launch_kwargs.pop("channel", None)
                if user_data_dir is not None:
                    context = await playwright.chromium.launch_persistent_context(
                        str(user_data_dir),
                        user_agent=user_agent,
                        viewport={"width": 1365, "height": 900},
                        ignore_https_errors=True,
                        **launch_kwargs,
                    )
                else:
                    browser = await playwright.chromium.launch(**launch_kwargs)
                    context = await browser.new_context(
                        user_agent=user_agent,
                        viewport={"width": 1365, "height": 900},
                        ignore_https_errors=True,
                        storage_state=storage_state,
                    )
            else:
                raise

        page = await context.new_page()
        try:
            for index, (row_index, row_key, url) in enumerate(jobs):
                if STOP_REQUESTED:
                    logging.info("Stopping browser crawl due to interrupt.")
                    break
                try:
                    logging.info("Browser detail %s/%s: %s", index + 1, len(jobs), url)
                    html = ""
                    for attempt in range(1, 4):
                        await page.goto(url, wait_until="domcontentloaded", timeout=DEFAULT_TIMEOUT_MS)
                        html = await wait_for_real_page_html(page, timeout_seconds=max(10.0, float(headful_wait or 0) + 10.0))

                        if html and not is_cf_challenge(html):
                            extracted = extract_specialized_field(html)
                            await orchestrator.record_result(row_index, row_key, extracted)
                            logging.info(
                                "Updated %s | frontage=%s | road_width=%s",
                                url,
                                extracted["frontage"],
                                extracted["road_width"],
                            )
                            break

                        if attempt < 3:
                            backoff = DEFAULT_RETRY_BASE_BACKOFF * (2 ** (attempt - 1)) + random.uniform(0, 1)
                            await asyncio.sleep(backoff)
                    else:
                        failed.append((row_index, row_key, url))
                        logging.warning("Browser crawl failed for %s", url)

                except Exception as exc:
                    logging.warning("Browser crawl failed for %s: %s", url, exc)
                    failed.append((row_index, row_key, url))

                await asyncio.sleep(random.uniform(DEFAULT_DELAY_MIN, DEFAULT_DELAY_MAX))
        finally:
            with suppress(Exception):
                await page.close()
            with suppress(Exception):
                await context.close()
            if browser is not None:
                with suppress(Exception):
                    await browser.close()

    return failed


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Specialized crawler for extracting frontage and road_width from batdongsan detail URLs."
    )
    parser.add_argument("--input", default=DEFAULT_INPUT_CSV, help="Input CSV path containing a `url` column")
    parser.add_argument("--progress-file", default=DEFAULT_PROGRESS_FILE, help="Progress checkpoint JSON path")
    parser.add_argument("--user-data-dir", default=DEFAULT_USER_DATA_DIR, help="Persistent browser profile directory")
    parser.add_argument("--proxy-file", default=DEFAULT_PROXY_FILE, help="Proxy list path")
    parser.add_argument("--timeout-ms", type=int, default=DEFAULT_TIMEOUT_MS, help="Page timeout for browser crawling")
    parser.add_argument("--concurrency", type=int, default=DEFAULT_CONCURRENCY, help="Concurrent URL workers")
    parser.add_argument("--delay-min", type=float, default=DEFAULT_DELAY_MIN, help="Minimum delay between requests")
    parser.add_argument("--delay-max", type=float, default=DEFAULT_DELAY_MAX, help="Maximum delay between requests")
    parser.add_argument("--autosave-batch-size", type=int, default=DEFAULT_AUTOSAVE_BATCH_SIZE, help="Rewrite CSV after this many successful updates")
    parser.add_argument("--max-retries", type=int, default=DEFAULT_MAX_RETRIES, help="Max retries for page fetch")
    parser.add_argument("--retry-backoff", type=float, default=DEFAULT_RETRY_BASE_BACKOFF, help="Base backoff seconds")
    parser.add_argument("--headful", action="store_true", help="Run browser in headful mode for manual anti-bot assist")
    parser.add_argument("--headful-wait", type=int, default=30, help="Seconds to wait in headful warm-up mode")
    parser.add_argument("--use-proxy", action="store_true", help="Enable proxy rotation")
    parser.add_argument("--overwrite", action="store_true", help="Overwrite rows that already have frontage or road_width")
    parser.add_argument("--log-level", default="INFO", help="Logging level")
    return parser.parse_args()

async def main_async() -> int:
    args = parse_args()
    logging.basicConfig(
        level=getattr(logging, args.log_level.upper(), logging.INFO),
        format="%(asctime)s [%(levelname)s] %(message)s",
    )
    global DEFAULT_TIMEOUT_MS
    DEFAULT_TIMEOUT_MS = max(1000, int(args.timeout_ms))
    input_csv = Path(args.input).resolve()
    progress_path = Path(args.progress_file).resolve()

    orchestrator = SpecializedCsvOrchestrator(
        csv_path=input_csv,
        progress_path=progress_path,
        batch_size=args.autosave_batch_size,
        overwrite=args.overwrite,
    )
    jobs = orchestrator.build_jobs()
    if not jobs:
        await orchestrator.flush()
        logging.info("No pending URLs. CSV is already up to date: %s", input_csv)
        return 0

    logging.info("Pending URLs: %d", len(jobs))

    # Keep the specialized crawler strictly sequential so only one product is fetched at a time.
    concurrency = 1
    if args.concurrency != 1:
        logging.info("Concurrency forced to 1 for sequential product crawling (requested: %d)", args.concurrency)

    proxy_pool = load_proxies(Path(args.proxy_file)) if args.use_proxy else []
    browser_proxy = proxy_pool[0] if proxy_pool else None

    selected_user_agent = random.choice(USER_AGENTS)
    user_data_dir = Path(args.user_data_dir).resolve()
    user_data_dir.mkdir(parents=True, exist_ok=True)
    warmup_urls = [url for _, _, url in jobs[:DEFAULT_WARMUP_COUNT]]
    cookies, cookie_state = await warm_up_playwright_session(
        warmup_urls,
        selected_user_agent,
        browser_proxy,
        user_data_dir,
        headful=args.headful,
        headful_wait=args.headful_wait,
    )
    if cookies.get("cf_clearance"):
        logging.info("cf_clearance acquired: %s...", cookies["cf_clearance"][:6])
    else:
        logging.warning("cf_clearance cookie not found after warm-up")

    if cookies.get("cf_clearance"):
        failed_jobs = await crawl_url_batch_httpx(
            jobs,
            cookies,
            selected_user_agent,
            concurrency,
            proxy_pool,
            orchestrator,
        )
        if failed_jobs:
            logging.info("HTTPX fallback to browser for %d URLs", len(failed_jobs))
            failed_jobs = await crawl_url_batch_browser(
                failed_jobs,
                selected_user_agent,
                concurrency,
                proxy_pool,
                orchestrator,
                args.headful_wait,
                args.headful,
                user_data_dir=user_data_dir,
                storage_state=build_playwright_storage_state(cookie_state) if cookie_state else None,
            )
    else:
        failed_jobs = await crawl_url_batch_browser(
            jobs,
            selected_user_agent,
            concurrency,
            proxy_pool,
            orchestrator,
            args.headful_wait,
            args.headful,
            user_data_dir=user_data_dir,
            storage_state=build_playwright_storage_state(cookie_state) if cookie_state else None,
        )

    for _, row_key, url in failed_jobs:
        logging.warning("URL failed after all retries: %s", url)
        await orchestrator.mark_failed(row_key)

    await orchestrator.flush()
    logging.info("Run completed. Updated CSV: %s", input_csv)
    return 0


def handle_sigint(signum, frame):
    global STOP_REQUESTED
    logging.info("Interrupt received, stopping gracefully... Please wait for current requests to finish.")
    STOP_REQUESTED = True

def main() -> None:
    signal.signal(signal.SIGINT, handle_sigint)
    signal.signal(signal.SIGTERM, handle_sigint)
    raise SystemExit(asyncio.run(main_async()))


if __name__ == "__main__":
    main()
