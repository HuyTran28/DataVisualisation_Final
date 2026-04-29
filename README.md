# DataVisualisation_Final

Crawler bất động sản cho batdongsan.com.vn dựa trên Crawl4AI + Pydantic v2 + CrawlDispatcher.

## Chạy nhanh

### Chế độ tuần tự (an toàn, mặc định)
```bash
pip install -r requirements.txt
python batdongsan_crawler.py --start-url "https://batdongsan.com.vn/nha-dat-ban/p2?vrs=1" --max-pages 1 --output output/batdongsan.csv
```

### Chế độ song song (nhanh hơn, cần chú ý)
```bash
python batdongsan_crawler.py \
  --start-url "https://batdongsan.com.vn/nha-dat-ban/p2?vrs=1" \
  --max-pages 5 \
  --output output/batdongsan.csv \
  --concurrency 3 \
  --delay-min 0.5 \
  --delay-max 1.2
```

### Chế độ với Rotating Proxy
```bash
python batdongsan_crawler.py 
  --start-url "https://batdongsan.com.vn/nha-dat-ban/p2?vrs=1" 
  --max-pages 10 
  --output output/batdongsan.csv 
  --concurrency 5 
  --use-proxy 
  --delay-min 0.3 
  --delay-max 0.8
```

## Tùy chọn Command Line

| Tùy chọn | Mô tả | Mặc định |
|----------|-------|---------|
| `--start-url` | URL trang danh mục bắt đầu | https://batdongsan.com.vn/nha-dat-ban/p2?vrs=1 |
| `--max-pages` | Số trang danh mục cần quét | 1 |
| `--output` | Đường dẫn file CSV output | batdongsan_real_estate.csv |
| `--concurrency` | Số request song song (1-10) | 3 |
| `--delay-min` | Độ trễ tối thiểu (giây) | 0.7 |
| `--delay-max` | Độ trễ tối đa (giây) | 1.8 |
| `--use-proxy` | Bật xoay vòng proxy | Tắt (không dùng) |
| `--log-level` | Mức độ log (DEBUG/INFO/WARNING/ERROR) | INFO |

## Cấu hình Proxy

### 1. Không dùng Proxy (Mặc định)
- An toàn nhất, tốc độ chậm
- Phù hợp: < 100 tin/ngày

### 2. Dùng Proxy (Bật --use-proxy)
Tạo file `proxies.txt` trong cùng thư mục với script:
```
http://ip1:port1
http://user:pass@ip2:port2
http://ip3:port3
```

Hoặc để trống để sử dụng fallback (không proxy).

### 3. Proxy Services Được Khuyên
- **Bright Data**: brightdata.com (ổn định, đắt tiền)
- **Smartproxy**: smartproxy.com (rẻ hơn, tốc độ tốt)
- **Oxylabs**: oxylabs.io (chất lượng cao)
- **Apify**: apify.com (tích hợp sẵn, dễ dùng)

## Chiến lược An toàn

| Tốc độ | Concurrency | Proxy | Delay | Lưu ý |
|--------|------------|-------|-------|-------|
| Chậm (An toàn) | 1 | Không | 1-2s | Không bị ban, < 100/ngày |
| Trung bình | 3 | Không | 0.7-1.8s | Vừa tốc độ, vừa an toàn |
| Nhanh | 5 | Có | 0.3-0.8s | Cần proxy, 500+ tin/ngày |
| Cực nhanh | 8-10 | Có (Premium) | 0.1-0.3s | Chi phí proxy cao, dùng khi cần |

## Xử lý Lỗi

### Lỗi 403 (Access Denied)
- Trang web chặn IP
- **Giải pháp**: Dùng proxy, giảm concurrency, tăng delay

### Timeout / Kết nối bị đứt
- Proxy xấu hoặc quá tải
- **Giải pháp**: Thử proxy khác, tăng delay, giảm concurrency

### Không extract được dữ liệu
- HTML khác cấu trúc, site update
- **Giải pháp**: Kiểm tra logs, update regex pattern

## 🚀 Tính Năng Tối Ưu Hóa (v4.0)

**6 Chiến Lược Hiệu Năng**:

1. **Persistent Browser Context** - Tái sử dụng phiên trình duyệt
2. **Smart Concurrency with Warm-up** - Execution 2 pha (warm-up + scale)
3. **Resource Blocking** - Chặn ảnh/font/CSS (~80% tiết kiệm băng thông)
4. **Proxy Rotation** - Xoay vòng proxy tự động qua HTTPX
5. **Hybrid Fallback** - Cookie injection + HTTPX speed boost (10-40x nhanh hơn)
6. **Session Management** - Dispatcher thích ứng + cache hiệu quả

**Cải Thiện Hiệu Năng**:
- Băng thông: -80% (từ 100% → 20%)
- Tốc độ/trang: -90% (từ 5s → 300-500ms với HTTPX)
- Bộ nhớ: -50% (từ 300-500MB → ~150MB)
- Đồng thời: +150% (từ 1-4 → 4-10 requests song song)

📖 **[OPTIMIZATION_GUIDE.md](./OPTIMIZATION_GUIDE.md)** - Hướng dẫn chi tiết

---

## Lệnh Nhanh (Quick Start v4.0)

### Test Toàn Bộ Optimization (Recommended)
```bash
python batdongsan_crawler.py \
  --start-url "https://batdongsan.com.vn/nha-dat-ban/p2?vrs=1" \
  --max-pages 5 \
  --warmup-count 3 \
  --concurrency 4 \
  --delay-min 0.7 \
  --delay-max 1.8 \
  --log-level INFO \
  --output batdongsan_v4.csv
```

### Debug Mode (Xem Chi Tiết)
```bash
python batdongsan_crawler.py \
  --max-pages 1 \
  --warmup-count 2 \
  --concurrency 2 \
  --log-level DEBUG
```

### Với Proxy (Cần proxies.txt)
```bash
python batdongsan_crawler.py \
  --max-pages 10 \
  --concurrency 6 \
  --use-proxy \
  --proxy-file proxies.txt \
  --log-level INFO
```

---

## Ghi chú Kỹ Thuật

- Script dùng `AsyncWebCrawler` + `CrawlDispatcher` cho crawl song parallel
- **Warm-up Phase** (mới): Crawl 3 URLs đầu để lấy `cf_clearance` cookie từ Cloudflare
- **Scale Phase** (mới): Dùng HTTPX với cookie injection để crawl nhanh 10-40x
- **Resource Blocking** (mới): Chặn images/CSS/fonts → tiết kiệm 80% băng thông
- Hỗ trợ Rotating Proxy tự động quay vòng (round-robin)
- Persistent browser profile (.crawler_profile/) → tái sử dụng cookies
- Có stealth mode, fallback HTTP fetch để vượt chặn bot
- Dữ liệu chuẩn hóa theo `RealEstateSchema` Pydantic v2
- Export CSV với UTF-8-SIG (tương thích Excel)