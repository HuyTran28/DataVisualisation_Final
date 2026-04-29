# CSS Selectors Guide - v3.0 Extract Functions

Tài liệu này giải thích 8 hàm extraction mới dựa trên CSS selectors, được thêm vào **batdongsan_crawler.py v3.0**.

## 📋 Tổng Quan

| Hàm | Selector CSS | Trích xuất | Kiểu dữ liệu |
|-----|--------------|-----------|-------------|
| `clean_numeric()` | N/A | Parse số từ text | `Optional[float]` |
| `extract_address_from_selector()` | `.re__address-line-1` | Full address (Đường D4, Quận 7, ...) | `Optional[str]` |
| `extract_product_id_from_selector()` | `#product-detail-web[prid]` | Product ID (pr12345) | `Optional[str]` |
| `extract_price_total_from_selector()` | `.re__pr-short-info-item` | Giá tiền (45.65 tỷ → 45650 triệu) | `Optional[float]` |
| `extract_price_per_m2_from_selector()` | `.re__pr-short-info-item[/m²]` | Giá trên m² (194.44 triệu/m²) | `Optional[float]` |
| `extract_area_and_frontage_from_selector()` | `.re__pr-specs-content-item` | Diện tích & mặt tiền | `tuple[Optional[float], Optional[float]]` |
| `extract_post_rank_from_selector()` | `.re__pr-short-info-item[title="Loại tin"]` | Loại tin (Tin VIP, Nổi bật) | `Optional[str]` |
| `extract_price_trend_from_selector()` | `.re__chart-col.re__col-2 strong` | Xu hướng giá 1 năm (+23%) | `Optional[float]` |
| `extract_is_verified_from_selector()` | `.marking-product__KYC` | Xác thực (KYC verified) | `bool` |

---

## 1️⃣ `clean_numeric(text: str) -> Optional[float]`

**Mục đích**: Parse số từ chuỗi text, xử lý đơn vị và dấu phân cách.

### Đầu vào
```python
clean_numeric("45,65")                # "45,65" → 45.65
clean_numeric("~194,44 triệu/m²")    # "~194,44" → 194.44
clean_numeric("45.65 tỷ")             # "45.65" → 45.65
clean_numeric("1.234,56")             # "1.234,56" → 1234.56 (European format)
clean_numeric("invalid")              # None
```

### Logic
1. Loại bỏ ký tự đặc biệt: `~`, khoảng trắng
2. Trích xuất số đầu tiên: `[\d.,]+`
3. Xử lý dấu phân cách:
   - Nếu có cả phẩy và chấm:
     - Phẩy sau chấm: `1.234,56` → `1234.56` (European)
     - Chấm sau phẩy: `1,234.56` → `1234.56` (US)
   - Chỉ phẩy: `194,44` → `194.44` (Vietnam)
4. Convert sang `float`

### Lưu ý
- **Không** xử lý đơn vị (tỷ, triệu, m²) - chỉ return số
- Đơn vị được xử lý riêng trong các hàm extractor cụ thể
- Alias: `safe_decimal()` (tương thích ngược)

---

## 2️⃣ `extract_address_from_selector(html: str) -> Optional[str]`

**Mục đích**: Trích xuất địa chỉ đầy đủ từ `.re__address-line-1`

### HTML Ví dụ
```html
<div class="re__address-line-1">
  Đường D4, Phường Phú Mỹ, Quận 7, Hồ Chí Minh
</div>
```

### Đầu vào & Đầu ra
```python
extract_address_from_selector(html)
# Returns: "Đường D4, Phường Phú Mỹ, Quận 7, Hồ Chí Minh"
```

### Logic
1. BeautifulSoup select `.re__address-line-1`
2. Get text content và normalize
3. Return `None` nếu không tìm thấy

### Sử dụng trong `parse_detail_page()`
```python
full_address_css = extract_address_from_selector(html)
# Fallback: full_address = full_address_css or metrics.get("full_address") or ""
```

---

## 3️⃣ `extract_product_id_from_selector(html: str, url: str) -> Optional[str]`

**Mục đích**: Lấy product ID từ `#product-detail-web[prid]` attribute

### HTML Ví dụ
```html
<div id="product-detail-web" prid="123456789">
  ...
</div>
```

### Đầu vào & Đầu ra
```python
extract_product_id_from_selector(html, url)
# Returns: "123456789"
```

### Logic
1. Find `#product-detail-web` element
2. Get `prid` attribute value
3. **Fallback**: Nếu không có prid, extract từ URL regex `pr(\d+)`
4. Return `None` nếu không thành công

### Sử dụng trong `parse_detail_page()`
```python
product_id_css = extract_product_id_from_selector(html, url)
product_id = product_id_css or product_id_old or ""  # Merge strategy
```

---

## 4️⃣ `extract_price_total_from_selector(html: str) -> Optional[float]`

**Mục đích**: Trích xuất giá tiền từ `.re__pr-short-info-item` items

### HTML Ví dụ
```html
<div class="re__pr-short-info-item" title="Giá">
  ~45,65 tỷ
</div>
<div class="re__pr-short-info-item" title="Diện tích">
  830 m²
</div>
```

### Đầu vào & Đầu ra
```python
extract_price_total_from_selector(html)
# Returns: 45650.0  (45.65 tỷ * 1000 = 45650 triệu)
```

### Logic
1. Select tất cả `.re__pr-short-info-item`
2. Skip items có `title` chứa: "Loại tin", "Mã tin", "Ngày đăng"
3. Skip items không có digit
4. Trích xuất số bằng `clean_numeric()`
5. **Chuyển đổi đơn vị**:
   - Nếu chứa "ty" → nhân 1000 (convert tỷ → triệu)
   - Nếu chứa "trieu" hoặc "tr" → giữ nguyên
6. Return `None` nếu không tìm thấy giá

### Lưu ý
- **Luôn** normalize sang đơn vị triệu VNĐ
- Ví dụ: `45,65 tỷ` → `45650.0` triệu

### Sử dụng trong `parse_detail_page()`
```python
price_total_css = extract_price_total_from_selector(html)
price_total = price_total_css or metrics.get("price_total")
```

---

## 5️⃣ `extract_price_per_m2_from_selector(html: str) -> Optional[float]`

**Mục đích**: Trích xuất giá trên m² từ `.re__pr-short-info-item[/m²]`

### HTML Ví dụ
```html
<div class="re__pr-short-info-item">
  ~194,44 triệu/m²
</div>
```

### Đầu vào & Đầu ra
```python
extract_price_per_m2_from_selector(html)
# Returns: 194.44  (triệu/m²)
```

### Logic
1. Select `.re__pr-short-info-item` items
2. Tìm item chứa `/m²` hoặc `/m2`
3. Trích xuất số bằng `clean_numeric()`
4. Return `None` nếu không tìm thấy

### Lưu ý
- **Đã ở đơn vị triệu/m²** - không cần convert thêm
- Text pattern: `~194,44 triệu/m²` → `194.44`

---

## 6️⃣ `extract_area_and_frontage_from_selector(html: str) -> tuple[Optional[float], Optional[float]]`

**Mục đích**: Trích xuất diện tích & mặt tiền từ `.re__pr-specs-content-item`

### HTML Ví dụ
```html
<div class="re__pr-specs-content-item" title="Diện tích">
  830 m²
</div>
<div class="re__pr-specs-content-item" title="Mặt tiền">
  12 m
</div>
<div class="ext">
  Mặt tiền 6 m
</div>
```

### Đầu vào & Đầu ra
```python
area, frontage = extract_area_and_frontage_from_selector(html)
# Returns: (830.0, 12.0)
```

### Logic
1. Select `.re__pr-specs-content-item` items
2. Tìm item có `title` chứa "dien tich" → parse area
3. Tìm item có `title` chứa "mat tien" → parse frontage
4. **Fallback** (nếu không tìm frontage từ specs-content):
   - Select `.ext` element
   - Parse text → extract số
5. Return `tuple(area, frontage)` (có thể `None`)

### Lưu ý
- Area: Lấy từ "Diện tích" spec item
- Frontage: Lấy từ "Mặt tiền" spec item HOẶC `.ext` element (fallback)

---

## 7️⃣ `extract_post_rank_from_selector(html: str) -> Optional[str]`

**Mục đích**: Trích xuất loại tin từ `.re__pr-short-info-item[title="Loại tin"]`

### HTML Ví dụ
```html
<div class="re__pr-short-info-item" title="Loại tin">
  Tin VIP Kim Cương
</div>
```

### Đầu vào & Đầu ra
```python
extract_post_rank_from_selector(html)
# Returns: "Tin VIP Kim Cương"
```

### Logic
1. Select `.re__pr-short-info-item` items
2. Tìm item có `title` chứa "loai tin"
3. Get text content và normalize
4. Return `None` nếu không tìm thấy

### Giá trị phổ biến
- "Tin VIP Kim Cương"
- "Tin VIP Bạc"
- "Tin Nổi bật"
- "Tin thường"

---

## 8️⃣ `extract_price_trend_from_selector(html: str) -> Optional[float]`

**Mục đích**: Trích xuất xu hướng giá từ `.re__chart-col.re__col-2 strong`

### HTML Ví dụ
```html
<div class="re__chart-col re__col-2">
  <strong>+23%</strong>
</div>
```

### Đầu vào & Đầu ra
```python
extract_price_trend_from_selector(html)
# Returns: 23.0  (from "+23%")
```

### Logic
1. Select `.re__chart-col.re__col-2 strong`
2. Get text content (`"+23%"` hoặc `"-5%"`)
3. Parse số bằng `clean_numeric()`
4. Return `None` nếu không tìm thấy

### Format
- Dương: `"+23%"` → `23.0`
- Âm: `"-5%"` → `-5.0`
- Unit `%` được loại bỏ tự động

---

## 9️⃣ `extract_is_verified_from_selector(html: str) -> bool`

**Mục đích**: Kiểm tra xem tin đã được xác thực (KYC) hay chưa

### HTML Ví dụ
```html
<div class="marking-product__KYC">
  Đã xác thực
</div>
```

### Đầu vào & Đầu ra
```python
extract_is_verified_from_selector(html)
# Returns: True  (nếu có .marking-product__KYC)
# Returns: False (nếu không)
```

### Logic
1. Select `.marking-product__KYC` element
2. Return `True` nếu tìm thấy, `False` nếu không

### Lưu ý
- Luôn return `bool` (không bao giờ `None`)
- Nếu không tìm thấy → `False`

---

## 🔀 Merge Strategy trong `parse_detail_page()`

### Thứ tự ưu tiên
```
CSS Selectors > Regex Fallback > Text Extraction
```

### Ví dụ
```python
# === PHASE 1: CSS Selectors (Priority) ===
full_address_css = extract_address_from_selector(html)
price_total_css = extract_price_total_from_selector(html)

# === PHASE 2: Regex Fallback ===
specs = extract_specs_from_content(html)
price_min, price_max = extract_price_range(html)
metrics = parse_numeric_features(text, lines)

# === MERGE STRATEGY: CSS > Regex > Text ===
full_address = full_address_css or metrics.get("full_address") or ""
price_total = price_total_css or metrics.get("price_total")
```

---

## 📊 Field Mapping

| RealEstateSchema Field | Extract Function | Fallback |
|------------------------|------------------|----------|
| `prid` | `extract_product_id_from_selector()` | `extract_product_id()` (text) |
| `full_address` | `extract_address_from_selector()` | `metrics["full_address"]` |
| `price_total` | `extract_price_total_from_selector()` | `metrics["price_total"]` |
| `price_per_m2` | `extract_price_per_m2_from_selector()` | `metrics["price_per_m2"]` |
| `area` | `extract_area_and_frontage_from_selector()[0]` | `metrics["area"]` |
| `frontage` | `extract_area_and_frontage_from_selector()[1]` | `specs["frontage"]` \| `metrics["frontage"]` |
| `post_rank` | `extract_post_rank_from_selector()` | `""` |
| `price_trend_1y` | `extract_price_trend_from_selector()` | `None` |
| `is_verified` | `extract_is_verified_from_selector()` | Text pattern match |

---

## ⚙️ Debugging Tips

### Enable DEBUG logging
```bash
python batdongsan_crawler.py --start-url "https://..." --log-level DEBUG
```

### Test single extractor
```python
from batdongsan_crawler import extract_price_total_from_selector
from bs4 import BeautifulSoup

html = """..."""  # Your HTML
result = extract_price_total_from_selector(html)
print(result)  # Debug output
```

### Check CSS selector validity
```python
from bs4 import BeautifulSoup

soup = BeautifulSoup(html, "html.parser")
items = soup.select(".re__pr-short-info-item")
print(f"Found {len(items)} items")  # Should be > 0

for item in items:
    print(item.get("title"), ":", item.get_text())
```

---

## 🚀 Usage in Script

```bash
# Standard run
python batdongsan_crawler.py --start-url "https://batdongsan.com.vn/nha-dat-ban/p1" \
    --max-pages 2 --concurrency 1 --output output.csv

# With proxy (if proxies.txt configured)
python batdongsan_crawler.py --start-url "https://..." \
    --use-proxy --max-pages 5 --output output.csv

# Debug mode
python batdongsan_crawler.py --start-url "https://..." \
    --max-pages 1 --concurrency 1 --log-level DEBUG
```

---

## 📝 Version History

| Version | Changes |
|---------|---------|
| v3.0 | +9 CSS selector extraction functions, new `clean_numeric()` helper |
| v2.0 | Hybrid extraction (CSS + Regex), 7 new schema fields |
| v1.0 | Initial release, regex-only extraction |

---

**Última atualização**: 2024 | **Language**: Tiếng Việt + English
