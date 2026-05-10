"""
Data Profiler Module
====================
Đọc file CSV và trích xuất schema (lược đồ dữ liệu) để inject vào
System Prompt của LLM, giúp model hiểu cấu trúc dữ liệu trước khi sinh code.
"""

import pandas as pd
import os
import json
from pathlib import Path


class DataProfiler:
    """
    Quản lý việc đọc và phân tích cấu trúc dữ liệu từ file CSV.
    Schema được cache lại sau lần đọc đầu tiên để tối ưu hiệu suất.
    """

    def __init__(self, csv_path: str):
        """
        Khởi tạo DataProfiler với đường dẫn tới file CSV.
        
        Args:
            csv_path: Đường dẫn tuyệt đối hoặc tương đối tới file CSV.
        """
        self.csv_path = csv_path
        self._df: pd.DataFrame | None = None
        self._schema_text: str | None = None
        self._schema_dict: dict | None = None

    def load_dataframe(self) -> pd.DataFrame:
        """
        Đọc file CSV vào DataFrame và cache lại.
        Chỉ đọc 1 lần duy nhất khi khởi động server.
        
        Returns:
            pd.DataFrame: DataFrame chứa toàn bộ dữ liệu.
        
        Raises:
            FileNotFoundError: Nếu file CSV không tồn tại.
        """
        if self._df is None:
            abs_path = Path(self.csv_path).resolve()
            if not abs_path.exists():
                raise FileNotFoundError(
                    f"Không tìm thấy file CSV: {abs_path}"
                )
            self._df = pd.read_csv(str(abs_path), encoding="utf-8")
        return self._df

    def get_dataframe_copy(self) -> pd.DataFrame:
        """
        Trả về một bản sao (deep copy) của DataFrame.
        Dùng cho mỗi lần exec() để tránh ô nhiễm dữ liệu gốc.
        
        Returns:
            pd.DataFrame: Bản sao an toàn của DataFrame.
        """
        df = self.load_dataframe()
        return df.copy(deep=True)

    def extract_schema(self) -> dict:
        """
        Trích xuất schema từ DataFrame: tên cột, kiểu dữ liệu,
        số null, và 3 dòng dữ liệu mẫu.
        
        Returns:
            dict: Schema dưới dạng dictionary.
        """
        if self._schema_dict is not None:
            return self._schema_dict

        df = self.load_dataframe()

        columns_info = []
        for col in df.columns:
            col_info = {
                "name": col,
                "dtype": str(df[col].dtype),
                "null_count": int(df[col].isnull().sum()),
                "null_percent": round(df[col].isnull().mean() * 100, 2),
                "unique_count": int(df[col].nunique()),
            }
            # Thêm thống kê cho cột số
            if df[col].dtype in ["float64", "int64"]:
                col_info["min"] = float(df[col].min()) if not pd.isna(df[col].min()) else None
                col_info["max"] = float(df[col].max()) if not pd.isna(df[col].max()) else None
                col_info["mean"] = round(float(df[col].mean()), 2) if not pd.isna(df[col].mean()) else None
            # Thêm giá trị mẫu cho cột phân loại
            elif df[col].dtype == "object":
                top_values = df[col].dropna().value_counts().head(5).index.tolist()
                col_info["sample_values"] = top_values

            columns_info.append(col_info)

        # Lấy 3 dòng đầu tiên làm dữ liệu mẫu
        sample_rows = df.head(3).fillna("NaN").to_dict(orient="records")

        self._schema_dict = {
            "total_rows": len(df),
            "total_columns": len(df.columns),
            "columns": columns_info,
            "sample_rows": sample_rows,
        }
        return self._schema_dict

    def build_system_prompt(self) -> str:
        """
        Xây dựng System Prompt chứa schema để inject vào LLM.
        Prompt này giúp model hiểu rõ cấu trúc dữ liệu và sinh code chính xác.
        
        Returns:
            str: System prompt đầy đủ.
        """
        if self._schema_text is not None:
            return self._schema_text

        schema = self.extract_schema()

        # Xây dựng bảng mô tả các cột
        columns_desc = []
        for col in schema["columns"]:
            desc = f"  - `{col['name']}` ({col['dtype']})"
            desc += f" | {col['null_count']} nulls ({col['null_percent']}%)"
            desc += f" | {col['unique_count']} unique values"
            if "min" in col and col["min"] is not None:
                desc += f" | range: [{col['min']}, {col['max']}] | mean: {col['mean']}"
            if "sample_values" in col:
                samples = ", ".join([f'"{v}"' for v in col["sample_values"][:3]])
                desc += f" | ví dụ: {samples}"
            columns_desc.append(desc)

        columns_text = "\n".join(columns_desc)

        # Format 3 dòng mẫu (chỉ lấy các cột quan trọng để tiết kiệm token)
        important_cols = [
            "city", "district", "price_total", "price_per_m2", "area",
            "bedrooms", "bathrooms", "floors", "direction", "legal_status",
            "has_elevator", "near_park", "is_frontage_road", "price_trend_1y"
        ]
        sample_text_parts = []
        for i, row in enumerate(schema["sample_rows"]):
            filtered = {k: v for k, v in row.items() if k in important_cols}
            sample_text_parts.append(f"  Row {i}: {json.dumps(filtered, ensure_ascii=False)}")
        sample_text = "\n".join(sample_text_parts)

        self._schema_text = f"""Bạn là trợ lý AI chuyên về phân tích dữ liệu bất động sản Việt Nam.
Bạn có quyền truy cập vào một DataFrame tên `df` (pandas) đã được load sẵn trong bộ nhớ.

## THÔNG TIN DATASET
- Tên: batdongsan_with_features.csv
- Chủ đề: Dữ liệu thị trường bất động sản thứ cấp tại Việt Nam
- Tổng số dòng: {schema['total_rows']:,}
- Tổng số cột: {schema['total_columns']}

## CẤU TRÚC CỘT (SCHEMA)
{columns_text}

## DỮ LIỆU MẪU (3 dòng đầu, các cột chính)
{sample_text}

## QUY TẮC PHẢN HỒI (BẮT BUỘC TUÂN THỦ)
**TRƯỜNG HỢP 1: NGƯỜI DÙNG CHỈ HỎI XIN Ý TƯỞNG, TƯ VẤN HOẶC GỢI Ý**
- Nếu người dùng chưa có yêu cầu phân tích cụ thể (ví dụ: "tôi nên phân tích gì", "gợi ý cho tôi vài biểu đồ"), hãy trả lời bằng ngôn ngữ tự nhiên bình thường.
- Đưa ra các gợi ý, phương pháp để người dùng lựa chọn.
- **TUYỆT ĐỐI KHÔNG** sinh ra khối lệnh ````python ... ```` trong trường hợp này.

**TRƯỜNG HỢP 2: NGƯỜI DÙNG YÊU CẦU PHÂN TÍCH, VẼ BIỂU ĐỒ HOẶC VIẾT CODE**
Nếu người dùng yêu cầu phân tích dữ liệu cụ thể, bạn bắt buộc tuân thủ các quy tắc sinh code sau:
1. **Chỉ sử dụng** các thư viện: `pandas` (as `pd`), `matplotlib.pyplot` (as `plt`), `seaborn` (as `sns`), `numpy` (as `np`).
2. DataFrame đã được load sẵn với tên biến `df`. **KHÔNG được** dùng `pd.read_csv()` hay đọc file.
3. **KHÔNG được** import thêm bất kỳ thư viện nào (đặc biệt `os`, `sys`, `subprocess`).
4. **KHÔNG được** sử dụng `open()`, `eval()`, `exec()`, `__import__()`.
5. Nếu vẽ biểu đồ: dùng `plt.figure()` → vẽ → `plt.tight_layout()` → `plt.show()`.
6. Nếu trả về bảng dữ liệu: gán kết quả vào biến `result_df`.
7. **BẮT BUỘC** thêm comment tiếng Việt giải thích chức năng của từng block code.
8. BẮT BUỘC TRÍCH XUẤT CÁC THAM SỐ: Tất cả các giá trị có thể tùy biến (như số lượng limit/head, tiêu đề biểu đồ, màu sắc biểu đồ) PHẢI được định nghĩa trong mảng `parameters`. KHÔNG được hardcode chúng trong code.
9. Trả về KẾT QUẢ DUY NHẤT LÀ MỘT CHUỖI JSON ĐÚNG CHUẨN, KHÔNG CÓ BẤT KỲ VĂN BẢN NÀO NẰM NGOÀI JSON. Cấu trúc JSON bắt buộc như sau:
```json
{{
  "parameters": [
    {{
      "name": "TOP_K",
      "type": "int",
      "value": 10,
      "label": "Số lượng hiển thị"
    }},
    {{
      "name": "CHART_COLOR",
      "type": "str",
      "value": "blue",
      "label": "Màu sắc biểu đồ",
      "options": ["blue", "green", "red", "purple", "orange", "cyan"] 
    }}
  ],
  "code": "Mã Python thực thi. Sử dụng các biến từ parameters (ví dụ TOP_K, CHART_COLOR) mà KHÔNG cần khởi tạo lại.",
  "explanation": "Giải thích mã bằng tiếng Việt"
}}
```
*Lưu ý: Với các tham số như màu sắc hoặc loại biểu đồ, hãy cung cấp thêm một mảng `"options"` chứa các giá trị hợp lệ để UI hiển thị dropdown list.*
"""
        return self._schema_text
