"""
LLM Client Module
==================
Wrapper để gọi Google Gemini API (sử dụng google-genai SDK mới).
Nhận prompt từ user, kết hợp với system prompt (chứa schema),
gửi tới LLM và parse kết quả trả về thành code + giải thích.
"""

import re
import os
from google import genai
from dotenv import load_dotenv


class LLMClient:
    """Client gọi Google Gemini API (hoặc Grok API) để sinh code phân tích dữ liệu."""

    def __init__(self, provider: str = "gemini", api_key: str | None = None, model_name: str | None = None):
        """
        Khởi tạo LLM Client.

        Args:
            provider: 'gemini' hoặc 'grok'
            api_key: API key. Nếu None, đọc từ biến môi trường.
            model_name: Tên model. Mặc định tùy provider.
        """
        load_dotenv()
        self.provider = provider.lower()
        
        if self.provider == "gemini":
            self.api_key = api_key or os.getenv("GEMINI_API_KEY", "")
            self.model_name = model_name or os.getenv("GEMINI_MODEL", "gemini-2.0-flash")

            if not self.api_key:
                raise ValueError(
                    "GEMINI_API_KEY chưa được cấu hình. "
                    "Vui lòng điền API key vào file .env hoặc truyền trực tiếp."
                )
            self.client = genai.Client(api_key=self.api_key)

        elif self.provider == "grok":
            self.api_key = api_key or os.getenv("GROK_API_KEY", "")
            self.model_name = model_name or os.getenv("GROK_MODEL", "grok-2-latest")
            
            if not self.api_key:
                raise ValueError(
                    "GROK_API_KEY chưa được cấu hình. "
                    "Vui lòng điền API key vào file .env hoặc truyền trực tiếp."
                )
            import openai
            self.client = openai.OpenAI(
                api_key=self.api_key,
                base_url="https://api.x.ai/v1",
            )
        else:
            raise ValueError(f"Provider không hợp lệ: {provider}. Chọn 'gemini' hoặc 'grok'.")

    def generate_code(self, user_prompt: str, system_prompt: str) -> dict:
        """
        Gửi prompt tới API và parse kết quả.
        Tự động retry nếu gặp rate limit (429).

        Returns:
            dict: {code, explanation, raw_response, [error]}
        """
        import time

        full_prompt = f"{system_prompt}\n\n## YÊU CẦU CỦA NGƯỜI DÙNG\n{user_prompt}"
        max_retries = 3

        for attempt in range(max_retries):
            try:
                if self.provider == "gemini":
                    response = self.client.models.generate_content(
                        model=self.model_name,
                        contents=full_prompt,
                    )
                    raw_text = response.text
                elif self.provider == "grok":
                    response = self.client.chat.completions.create(
                        model=self.model_name,
                        messages=[
                            {"role": "system", "content": system_prompt},
                            {"role": "user", "content": user_prompt},
                        ],
                    )
                    raw_text = response.choices[0].message.content

                parsed_data = self._parse_response(raw_text)

                return {
                    "code": parsed_data["code"],
                    "explanation": parsed_data["explanation"],
                    "parameters": parsed_data.get("parameters", []),
                    "raw_response": raw_text,
                }
            except Exception as e:
                error_str = str(e)
                # Retry nếu gặp rate limit (429)
                if "429" in error_str or "RESOURCE_EXHAUSTED" in error_str:
                    wait_time = 5 * (2 ** attempt)  # 5s, 10s, 20s
                    print(f"⏳ Rate limit hit, retry {attempt+1}/{max_retries} sau {wait_time}s...")
                    if attempt < max_retries - 1:
                        time.sleep(wait_time)
                        continue

                return {
                    "code": "",
                    "explanation": f"Lỗi khi gọi {self.provider.capitalize()} API: {error_str}",
                    "parameters": [],
                    "raw_response": "",
                    "error": error_str,
                }

    def _parse_response(self, raw_text: str) -> dict:
        """Parse response từ LLM để lấy code, explanation và parameters."""
        import json
        
        # Thử tìm chuỗi JSON (bỏ qua markdown json ```json ... ```)
        json_pattern = r"```(?:json)?\s*(\{.*?\})\s*```"
        match = re.search(json_pattern, raw_text, re.DOTALL)
        json_str = match.group(1) if match else raw_text
        
        # Loại bỏ các ký tự rác nếu có ở đầu/cuối
        start_idx = json_str.find('{')
        end_idx = json_str.rfind('}')
        if start_idx != -1 and end_idx != -1 and end_idx >= start_idx:
            json_str = json_str[start_idx:end_idx+1]
        
        try:
            parsed = json.loads(json_str)
            return {
                "code": parsed.get("code", ""),
                "explanation": parsed.get("explanation", "Không có giải thích."),
                "parameters": parsed.get("parameters", [])
            }
        except json.JSONDecodeError:
            # Fallback về phương pháp cũ nếu LLM không trả về JSON hợp lệ
            code_pattern = r"```python\s*\n(.*?)```"
            code_matches = re.findall(code_pattern, raw_text, re.DOTALL)
            code = code_matches[0].strip() if code_matches else ""
            
            if not code:
                return {"code": "", "explanation": raw_text.strip(), "parameters": []}
            
            explanation_pattern = r"\*\*Giải thích:\*\*\s*(.*)"
            exp_match = re.search(explanation_pattern, raw_text, re.DOTALL)
            if exp_match:
                explanation = exp_match.group(1).strip()
            else:
                parts = raw_text.split("```")
                explanation = parts[-1].strip() if len(parts) > 2 else "Không có giải thích chi tiết."
            
            return {
                "code": code,
                "explanation": explanation,
                "parameters": []
            }
