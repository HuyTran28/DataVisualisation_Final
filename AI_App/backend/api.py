"""
FastAPI Backend - AI Assistant cho EDA Bất Động Sản
===================================================
Cung cấp các API endpoints:
  - POST /api/generate : Sinh code từ LLM
  - POST /api/execute  : Thực thi code đã duyệt
  - POST /api/logs     : Ghi log phiên tương tác
  - GET  /api/health   : Health check
  - GET  /api/schema   : Trả schema dataset
"""

import os
import sys
from pathlib import Path
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from dotenv import load_dotenv

# Thêm thư mục gốc dự án vào path
PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

# Load biến môi trường từ AI_App/.env
env_path = Path(__file__).resolve().parent.parent / ".env"
load_dotenv(dotenv_path=env_path)

from AI_App.backend.data_profiler import DataProfiler
from AI_App.backend.llm_client import LLMClient
from AI_App.backend.executor import CodeExecutor
from AI_App.backend.logger import SessionLogger

# =============================================
# Biến toàn cục (khởi tạo khi startup)
# =============================================
profiler: DataProfiler = None
executor: CodeExecutor = None
llm_client: LLMClient = None
session_logger: SessionLogger = None
system_prompt: str = ""


# =============================================
# Startup / Shutdown Events
# =============================================
@asynccontextmanager
async def lifespan(app: FastAPI):
    """Khởi tạo các service khi server start."""
    global profiler, executor, llm_client, session_logger, system_prompt

    # Xác định đường dẫn CSV
    csv_path = os.getenv("CSV_PATH", "Dataset/batdongsan_with_features.csv")
    abs_csv = (PROJECT_ROOT / csv_path).resolve()
    print(f"📂 Loading CSV: {abs_csv}")

    # Khởi tạo Data Profiler và load dữ liệu
    profiler = DataProfiler(str(abs_csv))
    df = profiler.load_dataframe()
    print(f"✅ Loaded DataFrame: {df.shape[0]:,} rows × {df.shape[1]} columns")

    # Xây dựng system prompt chứa schema
    system_prompt = profiler.build_system_prompt()
    print(f"✅ System prompt built ({len(system_prompt):,} chars)")

    # Khởi tạo Executor
    executor = CodeExecutor(df)
    print("✅ Code Executor ready")

    # Khởi tạo LLM Client
    provider = os.getenv("LLM_PROVIDER", "gemini").lower()
    if provider == "gemini":
        api_key = os.getenv("GEMINI_API_KEY", "")
    elif provider == "grok":
        api_key = os.getenv("GROK_API_KEY", "")
    else:
        api_key = ""

    if api_key:
        try:
            llm_client = LLMClient(provider=provider, api_key=api_key)
            print(f"✅ {provider.capitalize()} LLM Client ready")
        except Exception as e:
            llm_client = None
            print(f"⚠️  Không thể khởi tạo {provider.capitalize()} Client: {e}")
            if provider == "gemini":
                print("   Thử chạy: conda install -c conda-forge certifi")
            elif provider == "grok":
                print("   Thử chạy: pip install openai")
    else:
        llm_client = None
        print(f"⚠️  {provider.upper()}_API_KEY chưa được cấu hình. Endpoint /api/generate sẽ không hoạt động.")

    # Khởi tạo Logger
    session_logger = SessionLogger()
    print("✅ Session Logger ready")
    print("🚀 Backend is ready!\n")

    yield  # Server chạy ở đây

    print("🛑 Shutting down backend...")


# =============================================
# FastAPI App
# =============================================
app = FastAPI(
    title="AI Assistant - EDA Bất Động Sản Việt Nam",
    description="Backend API cho module AI hỗ trợ phân tích dữ liệu bất động sản",
    version="1.0.0",
    lifespan=lifespan,
)

# CORS - cho phép Streamlit frontend kết nối
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# =============================================
# Pydantic Models (Request / Response)
# =============================================
class GenerateRequest(BaseModel):
    prompt: str  # Câu hỏi phân tích từ user

class ExecuteRequest(BaseModel):
    code: str  # Code Python đã được user phê duyệt

class LogRequest(BaseModel):
    user_prompt: str
    generated_code: str
    executed_code: str
    result_summary: str
    error: str = ""


# =============================================
# API Endpoints
# =============================================
@app.get("/api/health")
async def health_check():
    """Health check - kiểm tra server hoạt động."""
    return {
        "status": "ok",
        "llm_ready": llm_client is not None,
        "data_loaded": profiler is not None and profiler._df is not None,
        "rows": profiler._df.shape[0] if profiler and profiler._df is not None else 0,
    }


@app.get("/api/schema")
async def get_schema():
    """Trả về schema dataset dưới dạng JSON."""
    if profiler is None:
        raise HTTPException(status_code=503, detail="Data chưa được load")
    return profiler.extract_schema()


@app.post("/api/generate")
async def generate_code(request: GenerateRequest):
    """
    Sinh code Python từ LLM dựa trên prompt của user.
    System prompt chứa schema được inject tự động.
    """
    if llm_client is None:
        provider = os.getenv("LLM_PROVIDER", "gemini").upper()
        raise HTTPException(
            status_code=503,
            detail=f"{provider}_API_KEY chưa được cấu hình hoặc Client khởi tạo thất bại. Kiểm tra file AI_App/.env"
        )

    if not request.prompt.strip():
        raise HTTPException(status_code=400, detail="Prompt không được để trống")

    result = llm_client.generate_code(
        user_prompt=request.prompt,
        system_prompt=system_prompt,
    )

    if "error" in result:
        error_detail = result.get("error", "Unknown error")
        print(f"❌ LLM Error: {error_detail}")
        raise HTTPException(status_code=502, detail=f"Lỗi Gemini API: {error_detail}")

    return {
        "code": result["code"],
        "explanation": result["explanation"],
    }


@app.post("/api/execute")
async def execute_code(request: ExecuteRequest):
    """
    Thực thi code Python đã được user phê duyệt.
    Code chạy trong sandbox với bản sao DataFrame.
    """
    if executor is None:
        raise HTTPException(status_code=503, detail="Executor chưa sẵn sàng")

    if not request.code.strip():
        raise HTTPException(status_code=400, detail="Code không được để trống")

    result = executor.execute(request.code)
    return result


@app.post("/api/logs")
async def save_log(request: LogRequest):
    """Ghi log phiên tương tác."""
    if session_logger is None:
        raise HTTPException(status_code=503, detail="Logger chưa sẵn sàng")

    entry = session_logger.log_session(
        user_prompt=request.user_prompt,
        generated_code=request.generated_code,
        executed_code=request.executed_code,
        result_summary=request.result_summary,
        error=request.error,
    )
    return {"status": "logged", "entry": entry}


@app.get("/api/logs")
async def get_logs(limit: int = 50):
    """Đọc lịch sử log."""
    if session_logger is None:
        raise HTTPException(status_code=503, detail="Logger chưa sẵn sàng")
    return session_logger.get_logs(limit=limit)
