"""
Code Executor Module
====================
Thực thi code Python trong một namespace được kiểm soát (sandboxed).
Bảo vệ DataFrame gốc bằng cách tạo bản sao cho mỗi lần chạy.
Tự động phát hiện biểu đồ và chuyển đổi sang base64.
"""

import io
import re
import base64
import traceback
import pandas as pd
import numpy as np
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import seaborn as sns


# Danh sách các lệnh bị cấm để ngăn code injection
BLOCKED_PATTERNS = [
    r"\bimport\s+os\b", r"\bimport\s+sys\b", r"\bimport\s+subprocess\b",
    r"\bimport\s+shutil\b", r"\bfrom\s+os\b", r"\bfrom\s+sys\b",
    r"\bfrom\s+subprocess\b", r"\b__import__\s*\(", r"\beval\s*\(",
    r"\bexec\s*\(", r"\bopen\s*\(", r"\bcompile\s*\(",
    r"\bglobals\s*\(", r"\blocals\s*\(", r"\bgetattr\s*\(",
    r"\bsetattr\s*\(", r"\bdelattr\s*\(", r"\bbreakpoint\s*\(",
]

# Import patterns an toàn — sẽ bị strip trước khi validate
# (vì các thư viện này đã có sẵn trong namespace)
SAFE_IMPORT_PATTERNS = [
    r"^\s*import\s+matplotlib\.pyplot\s+as\s+plt\s*$",
    r"^\s*import\s+matplotlib\s*$",
    r"^\s*import\s+seaborn\s+as\s+sns\s*$",
    r"^\s*import\s+numpy\s+as\s+np\s*$",
    r"^\s*import\s+pandas\s+as\s+pd\s*$",
    r"^\s*from\s+matplotlib\s+import\s+.*$",
    r"^\s*import\s+matplotlib\..*$",
]

# Danh sách builtins an toàn
SAFE_BUILTINS = {
    "print": print, "len": len, "range": range, "enumerate": enumerate,
    "zip": zip, "map": map, "filter": filter, "sorted": sorted,
    "reversed": reversed, "min": min, "max": max, "sum": sum,
    "abs": abs, "round": round, "int": int, "float": float,
    "str": str, "bool": bool, "list": list, "dict": dict,
    "tuple": tuple, "set": set, "type": type, "isinstance": isinstance,
    "True": True, "False": False, "None": None,
}


class CodeExecutor:
    """Thực thi code Python an toàn trong sandbox."""

    def __init__(self, original_df: pd.DataFrame):
        self._original_df = original_df

    def _strip_safe_imports(self, code: str) -> str:
        """Loại bỏ các dòng import an toàn trước khi validate."""
        lines = code.split("\n")
        filtered = []
        for line in lines:
            is_safe = any(re.match(p, line) for p in SAFE_IMPORT_PATTERNS)
            if not is_safe:
                filtered.append(line)
        return "\n".join(filtered)

    def validate_code(self, code: str) -> tuple[bool, str]:
        """Kiểm tra code có chứa lệnh nguy hiểm không."""
        # Strip các import an toàn trước khi kiểm tra
        code_to_check = self._strip_safe_imports(code)
        for pattern in BLOCKED_PATTERNS:
            match = re.search(pattern, code_to_check)
            if match:
                return False, f"⛔ Code chứa lệnh bị cấm: `{match.group()}`."
        return True, ""

    def execute(self, code: str) -> dict:
        """
        Thực thi code trong namespace an toàn.
        Returns: {success, result_type, data, stdout}
        """
        is_safe, error_msg = self.validate_code(code)
        if not is_safe:
            return {"success": False, "result_type": "error", "data": error_msg, "stdout": ""}

        df_copy = self._original_df.copy(deep=True)
        stdout_capture = io.StringIO()

        def captured_print(*args, **kwargs):
            output = io.StringIO()
            kwargs["file"] = output
            print(*args, **kwargs)
            stdout_capture.write(output.getvalue())

        safe_builtins = {**SAFE_BUILTINS, "print": captured_print}
        exec_namespace = {
            "pd": pd, "np": np, "plt": plt, "sns": sns,
            "df": df_copy, "__builtins__": safe_builtins,
        }

        modified_code = code.replace("plt.show()", "pass  # plt.show() auto-handled")
        # Strip các import an toàn khỏi code vì đã có sẵn trong namespace
        modified_code = self._strip_safe_imports(modified_code)
        plt.close("all")

        try:
            exec(modified_code, exec_namespace)
            stdout_text = stdout_capture.getvalue()

            # Kiểm tra biểu đồ
            figures = [plt.figure(i) for i in plt.get_fignums()]
            if figures:
                chart_data = self._figures_to_base64(figures)
                plt.close("all")
                return {"success": True, "result_type": "chart", "data": chart_data, "stdout": stdout_text}

            # Kiểm tra result_df
            if "result_df" in exec_namespace:
                result = exec_namespace["result_df"]
                if isinstance(result, pd.Series):
                    result = result.to_frame()
                if isinstance(result, pd.DataFrame):
                    return {"success": True, "result_type": "dataframe",
                            "data": result.to_json(orient="records", force_ascii=False),
                            "stdout": stdout_text}

            return {"success": True, "result_type": "text",
                    "data": stdout_text if stdout_text else "✅ Code đã chạy thành công.",
                    "stdout": stdout_text}

        except SyntaxError as e:
            return {"success": False, "result_type": "error",
                    "data": f"❌ Lỗi cú pháp: {e}\nDòng {e.lineno}: {e.text}", "stdout": stdout_capture.getvalue()}
        except Exception as e:
            return {"success": False, "result_type": "error",
                    "data": f"❌ Lỗi runtime:\n{e}\n\n{traceback.format_exc()}", "stdout": stdout_capture.getvalue()}
        finally:
            plt.close("all")

    def _figures_to_base64(self, figures: list) -> str | list[str]:
        """Chuyển matplotlib figures thành base64 PNG."""
        results = []
        for fig in figures:
            buf = io.BytesIO()
            fig.savefig(buf, format="png", dpi=150, bbox_inches="tight", facecolor="white")
            buf.seek(0)
            results.append(base64.b64encode(buf.read()).decode("utf-8"))
            buf.close()
        return results[0] if len(results) == 1 else results
