from __future__ import annotations

import argparse
import csv
import re
import sys
from pathlib import Path


def normalize_text(text: str) -> str:
    return re.sub(r"\s+", " ", text).strip()


def normalize_line_breaks(text: str) -> str:
    text = text.replace("\r\n", "\n").replace("\r", "\n")
    text = re.sub(r"[ \t]+\n", "\n", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return text.strip()


def canonical(text: str) -> str:
    return normalize_text(text).casefold()


START_MARKERS = [
    "Thông tin mô tả",
    "Thong tin mo ta",
    "Mô tả",
    "Mo ta",
]

STOP_MARKERS = [
    "Batdongsan.com.vn đã xác thực",
    "Có sổ đỏ/hợp đồng mua bán",
    "Tìm hiểu thêm về Tin xác thực",
    "Đặc điểm bất động sản",
    "Thông tin dự án",
    "Lịch sử giá",
    "Tổng hợp xử lý bởi Batdongsan.com.vn",
    "Xem lịch sử giá",
    "Xem trên bản đồ",
    "Ngày đăng",
    "Ngày hết hạn",
    "Loại tin",
    "Mã tin",
    "Video",
    "Hình ảnh",
    "Hình ảnh",
    "Bản đồ / Bất động sản dành cho bạn",
    "Tin đăng đã xem",
    "Tìm kiếm theo từ khóa",
    "Quý vị đang xem nội dung tin rao",
    "Mọi thông tin, nội dung liên quan tới tin rao này",
    "Trường hợp phát hiện nội dung tin đăng không chính xác",
    "Chat qua Zalo",
    "Hiện số",
    "Hiện số",
    "Mua bán nhà đất tại",
    "Bất động sản nổi bật",
    "Hỗ trợ tiện ích",
    "Đã sao chép liên kết",
    "CÔNG TY CỔ PHẦN PROPERTYGURU VIỆT NAM",
    "HƯỚNG DẪN",
    "QUY ĐỊNH",
    "ĐĂNG KÝ NHẬN TIN",
    "Copyright ©",
    "Giấy ĐKKD số",
    "Trang thông tin điện tử tổng hợp Batdongsan.com.vn",
    "${optionOnboarding",
    "Bạn thấy thế nào về Tin xác thực của Batdongsan.com.vn?",
    "Chúng tôi đã cập nhật Chính sách bảo mật",
    "Batdongsan.com.vn Chào mừng bạn đến với Batdongsan.com.vn!",
    "Họ và tên *",
    "Số điện thoại *",
    "Email",
    "Bắt đầu chat",
    "Cancel",
    "Thả để chọn tập tin",
    "Phản hồi Trải nghiệm của bạn trên Batdongsan.com.vn thế nào?",
]

NOISE_PATTERNS = [
    r"\bTải ứng dụng\b",
    r"\bTin đăng đã lưu\b",
    r"\bTin đăng đã lưu\b",
    r"\bXem tất cả\b",
    r"\bĐăng nhập\b",
    r"\bĐăng ký\b",
    r"\bĐăng ký\b",
    r"\bĐăng tin\b",
    r"\bLưu tin thành công\b",
    r"\bĐã bỏ lưu tin\b",
    r"\bFacebook\b",
    r"\bSao chép liên kết\b",
]

START_MARKERS_CANONICAL = [canonical(marker) for marker in START_MARKERS]
STOP_MARKERS_CANONICAL = [canonical(marker) for marker in STOP_MARKERS]
NOISE_REGEXES = [re.compile(pattern, flags=re.IGNORECASE) for pattern in NOISE_PATTERNS]


def _find_marker_index(text: str, markers: list[str]) -> tuple[int, str] | None:
    lowered = text.casefold()
    best: tuple[int, str] | None = None
    for marker in markers:
        idx = lowered.find(marker.casefold())
        if idx >= 0 and (best is None or idx < best[0]):
            best = (idx, marker)
    return best


def _trim_between_markers(text: str) -> str:
    start_match = _find_marker_index(text, START_MARKERS)
    if start_match is not None:
        start_idx, marker = start_match
        text = text[start_idx + len(marker) :].strip(" :.-\n\t")

    stop_positions = [
        idx for marker in STOP_MARKERS for idx in [text.casefold().find(marker.casefold())] if idx >= 0
    ]
    if stop_positions:
        text = text[: min(stop_positions)]

    return text.strip()


def _remove_title_prefix(text: str, page_title: str) -> str:
    title = normalize_text(page_title)
    if not title:
        return text

    title_cf = title.casefold()
    text_cf = text.casefold()
    if text_cf.startswith(title_cf):
        return text[len(title) :].lstrip(" -:.")
    return text


def _remove_noise(text: str) -> str:
    cleaned = text
    for regex in NOISE_REGEXES:
        cleaned = regex.sub(" ", cleaned)
    cleaned = re.sub(r"\$\{[^}]+\}", " ", cleaned)
    cleaned = re.sub(r"\b\d+\s*/\s*\d+\b", " ", cleaned)
    cleaned = re.sub(r"\b0\s+1\b", " ", cleaned)
    return normalize_text(cleaned)


def _format_description(text: str) -> str:
    text = normalize_text(text)
    if not text:
        return ""

    text = re.sub(r"\s*-\s*-\s*", "\n- ", text)
    text = re.sub(r"(:)\s*-\s+", r"\1\n- ", text)
    text = re.sub(r"(\.)\s*-\s+", r"\1\n- ", text)
    text = re.sub(
        r"\s+(?=(Thông tin chi tiết:|Lợi thế vượt trội:|Tiện ích xung quanh:|Đặc biệt,|Vị trí đẹp,))",
        "\n",
        text,
        flags=re.IGNORECASE,
    )
    text = re.sub(r"(?<!\n)\s+(?=(?:\d+/|\*\s))", "\n", text)
    text = re.sub(r"\n{3,}", "\n\n", text)
    return normalize_line_breaks(text)


def clean_description(text: str, page_title: str = "") -> str:
    if not text:
        return normalize_text(page_title)

    working = normalize_line_breaks(text)
    working = _remove_title_prefix(working, page_title)
    working = _trim_between_markers(working)
    working = _remove_noise(working)
    working = _remove_title_prefix(working, page_title)

    cleaned = _format_description(working)
    return cleaned or normalize_text(page_title)


def process_csv(input_csv: Path, output_csv: Path) -> int:
    if not input_csv.exists():
        print(f"Error: Input file {input_csv} does not exist.")
        return 1

    rows_read = 0
    rows_written = 0

    try:
        with input_csv.open("r", encoding="utf-8-sig", newline="") as infile, output_csv.open(
            "w", encoding="utf-8-sig", newline=""
        ) as outfile:
            reader = csv.DictReader(infile)
            if not reader.fieldnames:
                print("Error: CSV file is empty or has no header.")
                return 1

            writer = csv.DictWriter(outfile, fieldnames=reader.fieldnames)
            writer.writeheader()

            for row in reader:
                rows_read += 1
                if "description" in row:
                    row["description"] = clean_description(
                        row.get("description", ""),
                        row.get("title", ""),
                    )
                writer.writerow(row)
                rows_written += 1

        print(f"Successfully processed {rows_written} rows.")
        print(f"Output written to: {output_csv}")
        return 0
    except Exception as exc:
        print(f"Error processing CSV after reading {rows_read} rows: {exc}")
        return 1


EXAMPLE_RAW = """Hạ 500 triệu - Chính chủ bán gấp nhà MT + nhà 3tầng phía sau dòng tiền sẵn Bình Chánh - còn 13.1tỷ Tải ứng dụng Tải ứng dụng 0 1 Tải ứng dụng Tải ứng dụng Tin đăng đã lưu Xem tất cả Đăng nhập Đăng ký Đăng tin Tin đăng đã lưu Nhà đất bán Bán căn hộ chung cư Bán chung cư mini, căn hộ dịch vụ Bán nhà riêng Bán nhà biệt thự, liền kề Bán nhà mặt phố Bán shophouse, nhà phố thương mại Bán đất nền dự án Bán đất Bán trang trại, khu nghỉ dưỡng Bán condotel Bán kho, nhà xưởng Bán loại bất động sản khác Nhà đất cho thuê Cho thuê căn hộ chung cư Cho thuê chung cư mini, căn hộ dịch vụ Cho thuê nhà riêng Cho thuê nhà biệt thự, liền kề Cho thuê nhà mặt phố Cho thuê shophouse, nhà phố thương mại Cho thuê nhà trọ, phòng trọ Cho thuê văn phòng Cho thuê, sang nhượng cửa hàng, ki ốt Cho thuê kho, nhà xưởng, đất Cho thuê loại bất động sản khác Thông tin mô tả C17/10 Đường Liên Ấp 2 - 3 - 4, Vĩnh Lộc A, Bình Chánh. Thông tin chi tiết: - Diện tích: 223.9m² có 180m² thổ cư (khuôn đất rộng, cực hiếm). - Kết cấu gồm 2 căn riêng biệt trên cùng khu đất: - Nhà phía trước: Nhà cấp 4 đang kinh doanh, có thu nhập ổn định. - Nhà phía sau: Nhà đúc kiên cố 3 tầng, có sân trước + sân sau thoáng mát. Lợi thế vượt trội: - Mặt tiền đường Liên Ấp khu dân cư đông đúc, buôn bán sầm uất. - Có sẵn dòng tiền cho thuê, không cần khai thác lại. - Khu vực đang phát triển mạnh, giá đất tăng đều (~14%/năm). Ngày đăng 28/04/2026"""


def _read_input(path: str | None) -> str:
    if not path:
        return sys.stdin.read()
    return Path(path).read_text(encoding="utf-8")


def main() -> int:
    parser = argparse.ArgumentParser(description="Clean Batdongsan listing descriptions")
    parser.add_argument("--input", help="Input CSV file path")
    parser.add_argument("--output", help="Output CSV file path with cleaned descriptions")
    parser.add_argument("--demo", action="store_true", help="Print a before/after example and exit")
    args = parser.parse_args()

    if args.demo:
        print("Original description (before):")
        print(EXAMPLE_RAW)
        print("\n" + "=" * 80 + "\n")
        print("Cleaned description (after):")
        print(clean_description(EXAMPLE_RAW))
        return 0

    if not args.input or not args.output:
        parser.print_help()
        return 1

    return process_csv(Path(args.input), Path(args.output))


if __name__ == "__main__":
    raise SystemExit(main())
