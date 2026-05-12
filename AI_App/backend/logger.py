"""
Logger Module
=============
Ghi log toàn bộ phiên tương tác vào file JSON (append-only).
Lưu vết: prompt, code gốc AI sinh, code user đã sửa, kết quả, timestamp.
"""

import json
import os
from datetime import datetime
from pathlib import Path


class SessionLogger:
    """Quản lý việc ghi log phiên tương tác vào file JSON."""

    def __init__(self, log_file: str = None):
        if log_file is None:
            log_file = os.path.join(
                os.path.dirname(os.path.abspath(__file__)), "logs.json"
            )
        self.log_file = log_file
        self._ensure_file_exists()

    def _ensure_file_exists(self):
        """Tạo file log nếu chưa tồn tại."""
        Path(self.log_file).parent.mkdir(parents=True, exist_ok=True)
        if not os.path.exists(self.log_file):
            with open(self.log_file, "w", encoding="utf-8") as f:
                json.dump([], f)

    def log_session(
        self,
        user_prompt: str,
        generated_code: str,
        executed_code: str,
        result_summary: str,
        error: str = "",
        explanation: str = "",
    ) -> dict:
        """
        Ghi một phiên tương tác vào log.

        Args:
            user_prompt: Câu hỏi gốc của user.
            generated_code: Code do AI sinh ra.
            executed_code: Code sau khi user chỉnh sửa và chạy.
            result_summary: Tóm tắt kết quả thực thi.
            error: Thông báo lỗi (nếu có).
            explanation: Giải thích từ AI.

        Returns:
            dict: Entry log vừa ghi.
        """
        entry = {
            "timestamp": datetime.now().isoformat(),
            "user_prompt": user_prompt,
            "explanation": explanation,
            "generated_code": generated_code,
            "executed_code": executed_code,
            "code_modified": generated_code.strip() != executed_code.strip(),
            "result_summary": result_summary[:500],  # Giới hạn kích thước
            "error": error,
        }

        try:
            with open(self.log_file, "r", encoding="utf-8") as f:
                logs = json.load(f)
        except (json.JSONDecodeError, FileNotFoundError):
            logs = []

        logs.append(entry)

        with open(self.log_file, "w", encoding="utf-8") as f:
            json.dump(logs, f, ensure_ascii=False, indent=2)

        return entry

    def get_logs(self, limit: int = 50) -> list[dict]:
        """Đọc các log gần nhất."""
        try:
            with open(self.log_file, "r", encoding="utf-8") as f:
                logs = json.load(f)
            return logs[-limit:]
        except (json.JSONDecodeError, FileNotFoundError):
            return []
