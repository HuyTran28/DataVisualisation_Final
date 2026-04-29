# Quick Reference - v3.0 CSS Extractors

## 🚀 One-Liner Usage

```python
# Import all new functions
from batdongsan_crawler import (
    clean_numeric, extract_address_from_selector, extract_product_id_from_selector,
    extract_price_total_from_selector, extract_price_per_m2_from_selector,
    extract_area_and_frontage_from_selector, extract_post_rank_from_selector,
    extract_price_trend_from_selector, extract_is_verified_from_selector
)

# Extract from HTML
address = extract_address_from_selector(html)                          # str
product_id = extract_product_id_from_selector(html, url)              # str
price_total = extract_price_total_from_selector(html)                 # float (triệu)
price_per_m2 = extract_price_per_m2_from_selector(html)               # float (triệu/m²)
area, frontage = extract_area_and_frontage_from_selector(html)        # float, float
post_rank = extract_post_rank_from_selector(html)                     # str
price_trend = extract_price_trend_from_selector(html)                 # float (%)
is_verified = extract_is_verified_from_selector(html)                 # bool

# Helper
num = clean_numeric("45,65 tỷ")                                       # 45.65
```

## 📍 CSS Selectors Cheat Sheet

| Function | CSS Selector | Pattern |
|----------|------------|---------|
| address | `.re__address-line-1` | Get text |
| product_id | `#product-detail-web[prid]` | Get attribute OR URL fallback |
| price_total | `.re__pr-short-info-item` | Find price item, convert tỷ→triệu |
| price_per_m2 | `.re__pr-short-info-item` + `/m²` | Find /m² item |
| area | `.re__pr-specs-content-item[title*="dien"]` | Title contains "dien tich" |
| frontage | `.re__pr-specs-content-item[title*="mat"]` OR `.ext` | Title contains "mat tien" |
| post_rank | `.re__pr-short-info-item[title*="loai"]` | Title contains "loai tin" |
| trend | `.re__chart-col.re__col-2 strong` | Get text +X% or -X% |
| verified | `.marking-product__KYC` | Element exists? → True/False |

## 🔄 Merge Strategy Priority

```
Phase 1 (CSS Selectors)  ─► Returns value? YES ─► Use it ─┐
                          └─ Returns None? ────────┐       │
                                                    ▼       │
Phase 2 (Regex Fallback) ─► Returns value? YES ─► Use it ─┤─────► Final Result
                          └─ Returns None? ────────┐       │
                                                    ▼       │
Phase 3 (Text Parsing)   ─► Returns value? YES ─► Use it ─┘
                          └─ Returns None? → None
```

## 💰 Price Normalization Examples

```python
clean_numeric("45,65 tỷ")              → 45.65
extract_price_total_from_selector(...)  → 45650.0  (×1000 for triệu)

clean_numeric("194,44 triệu/m²")       → 194.44
extract_price_per_m2_from_selector(...) → 194.44   (already triệu/m²)

clean_numeric("1.234,56")              → 1234.56  (European format)
clean_numeric("1,234.56")              → 1234.56  (US format)
clean_numeric("194,44")                → 194.44   (Vietnam format)
```

## 🐛 Debug Checklist

```python
# 1. Check if selector finds elements
soup = BeautifulSoup(html, "html.parser")
items = soup.select(".re__pr-short-info-item")
print(f"Items found: {len(items)}")  # Should be > 0

# 2. Check element content
for item in items:
    print(f"Title: {item.get('title')}, Text: {item.get_text()}")

# 3. Test individual extractor
from batdongsan_crawler import extract_price_total_from_selector
result = extract_price_total_from_selector(html)
print(f"Price: {result}")

# 4. Enable full DEBUG logging
# python batdongsan_crawler.py --log-level DEBUG
```

## 📊 Return Types Summary

| Function | Return Type | Example | Fallback |
|----------|------------|---------|----------|
| `clean_numeric()` | `Optional[float]` | `45.65` | `None` |
| `extract_address_from_selector()` | `Optional[str]` | `"Đ. D4, Q. 7"` | `None` |
| `extract_product_id_from_selector()` | `Optional[str]` | `"123456789"` | `None` |
| `extract_price_total_from_selector()` | `Optional[float]` | `45650.0` | `None` |
| `extract_price_per_m2_from_selector()` | `Optional[float]` | `194.44` | `None` |
| `extract_area_and_frontage_from_selector()` | `tuple[Optional[float], Optional[float]]` | `(830.0, 12.0)` | `(None, None)` |
| `extract_post_rank_from_selector()` | `Optional[str]` | `"Tin VIP Kim Cương"` | `None` |
| `extract_price_trend_from_selector()` | `Optional[float]` | `23.0` | `None` |
| `extract_is_verified_from_selector()` | `bool` | `True` / `False` | `False` |

## ⚡ Common Patterns

### Get all prices as triệu
```python
price_total = extract_price_total_from_selector(html)  # Already triệu
price_per_m2 = extract_price_per_m2_from_selector(html) # Already triệu
# Use as-is, no conversion needed
```

### Validate critical fields
```python
required_fields = {
    'product_id': extract_product_id_from_selector(html, url),
    'price': extract_price_total_from_selector(html),
    'area': extract_area_and_frontage_from_selector(html)[0],
}

for field, value in required_fields.items():
    if value is None:
        print(f"Missing: {field} - will use fallback extraction")
```

### Conditional extraction
```python
# Prefer CSS, fallback to regex
price = extract_price_total_from_selector(html) or specs.get('price_total')
area = extract_area_and_frontage_from_selector(html)[0] or metrics.get('area')
```

## 🔧 If Site HTML Changes

1. **Selector no longer valid**: Update the `soup.select()` call
2. **Element renamed**: Check browser DevTools for new selector
3. **Attribute moved**: Add fallback logic to extractor
4. **Test before deploy**: Run on single page with DEBUG logging

Example update:
```python
# Old
address_elem = soup.select_one(".re__address-line-1")

# New (if site uses different class)
address_elem = soup.select_one(".property-address, .address-header")
```

## 📚 Related Functions

### Already Implemented (Use as Fallback)
- `parse_numeric_features()` - Extract metrics from text
- `extract_specs_from_content()` - Regex specs extraction
- `parse_address_improved()` - Address parsing (post-extraction)
- `extract_description_flags()` - Boolean flags from description

### Call Sequence in `parse_detail_page()`
```
1. extract_address_from_selector(html)
2. extract_product_id_from_selector(html, url)
3. extract_price_total_from_selector(html)
4. extract_price_per_m2_from_selector(html)
5. extract_area_and_frontage_from_selector(html)
6. extract_post_rank_from_selector(html)
7. extract_price_trend_from_selector(html)
8. extract_is_verified_from_selector(html)
9. [FALLBACK] extract_specs_from_content(html)
10. [FALLBACK] extract_price_range(html)
11. [FALLBACK] parse_numeric_features(text, lines)
12. merge_results() + validate
13. create_RealEstateSchema()
```

## 🎯 Performance Notes

- **Extractors are fast**: Single selector operations ~1-2ms each
- **Parallel safe**: No shared state, can be called concurrently
- **Memory efficient**: Each function processes independently
- **No side effects**: Pure functions, safe to retry

## 📞 Troubleshooting Quick Answers

**Q: Why is price showing as None?**  
A: Check if `.re__pr-short-info-item` elements exist. Use DEBUG logging to see what's matched.

**Q: Why is address empty?**  
A: `.re__address-line-1` might not exist or be empty. Check HTML with browser DevTools.

**Q: Why does frontage calculation differ?**  
A: Might be using `.ext` fallback instead of spec item. Check DEBUG logs for phase information.

**Q: Can I use these in my own code?**  
A: Yes! Functions are pure and reusable. Just import and call with HTML string.

**Q: Do I need to update proxies or anything?**  
A: No! Extractors are pure HTML parsing. No network calls needed.

---

**v3.0 Quick Reference | Updated 2024**
