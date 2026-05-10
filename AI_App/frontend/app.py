"""
Streamlit Frontend - AI Assistant cho EDA Bất Động Sản
======================================================
Giao diện chat cho phép user:
  1. Đặt câu hỏi phân tích dữ liệu
  2. Review / chỉnh sửa code AI sinh ra
  3. Approve & Execute code
  4. Xem kết quả (biểu đồ / bảng / text)
"""

import streamlit as st
import requests
import json
import base64
import pandas as pd
from datetime import datetime

# =============================================
# CẤU HÌNH TRANG
# =============================================
st.set_page_config(
    page_title="AI Assistant – EDA Bất Động Sản",
    page_icon="🏠",
    layout="wide",
    initial_sidebar_state="expanded",
)

BACKEND_URL = "http://localhost:8000"

# =============================================
# CUSTOM CSS — tối ưu cho Dark theme
# =============================================
st.markdown("""
<style>
    /* ── Ẩn branding mặc định ── */
    #MainMenu, footer, header {visibility: hidden;}

    /* ── Nút bấm ── */
    div.stButton > button {
        border-radius: 8px;
        font-weight: 600;
        transition: all 0.2s ease;
    }
    div.stButton > button:hover {
        transform: translateY(-1px);
        box-shadow: 0 4px 12px rgba(0,0,0,0.3);
    }

    /* ── Code editor textarea ── */
    .stTextArea textarea {
        font-family: 'Consolas', 'Fira Code', 'Source Code Pro', monospace !important;
        font-size: 14px !important;
        line-height: 1.5 !important;
    }

    /* ── Sidebar ── */
    section[data-testid="stSidebar"] {
        border-right: 1px solid rgba(250,250,250,0.08);
    }

    /* ── Sidebar suggestion buttons ── */
    section[data-testid="stSidebar"] div.stButton > button {
        text-align: left;
        font-size: 0.85rem;
        padding: 0.4rem 0.75rem;
        border: 1px solid rgba(250,250,250,0.1);
        background: rgba(250,250,250,0.03);
    }
    section[data-testid="stSidebar"] div.stButton > button:hover {
        background: rgba(250,250,250,0.08);
        border-color: rgba(250,250,250,0.2);
    }
</style>
""", unsafe_allow_html=True)


# =============================================
# SESSION STATE
# =============================================
def init_session_state():
    defaults = {
        "messages": [],
        "pending_code": None,
        "explanation": None,
        "original_code": None,
        "last_result": None,
        "last_prompt": None,
        "backend_ok": None,
        "pending_parameters": [],
    }
    for key, val in defaults.items():
        if key not in st.session_state:
            st.session_state[key] = val

init_session_state()


# =============================================
# HELPER FUNCTIONS
# =============================================
def check_backend() -> bool:
    try:
        r = requests.get(f"{BACKEND_URL}/api/health", timeout=3)
        st.session_state.backend_ok = r.status_code == 200
        return st.session_state.backend_ok
    except Exception:
        st.session_state.backend_ok = False
        return False


def call_generate(prompt: str) -> dict | None:
    try:
        r = requests.post(f"{BACKEND_URL}/api/generate", json={"prompt": prompt}, timeout=60)
        if r.status_code == 200:
            return r.json()
        st.error(f"❌ Backend error: {r.json().get('detail', r.text)}")
    except requests.exceptions.ConnectionError:
        st.error("❌ Không kết nối được backend.")
    except Exception as e:
        st.error(f"❌ {e}")
    return None


def call_execute(code: str) -> dict | None:
    try:
        r = requests.post(f"{BACKEND_URL}/api/execute", json={"code": code}, timeout=120)
        if r.status_code == 200:
            return r.json()
        st.error(f"❌ Lỗi thực thi: {r.json().get('detail', r.text)}")
    except Exception as e:
        st.error(f"❌ {e}")
    return None


def call_log(user_prompt, generated_code, executed_code, result_summary, error="", explanation=""):
    try:
        requests.post(f"{BACKEND_URL}/api/logs", json={
            "user_prompt": user_prompt, "generated_code": generated_code,
            "executed_code": executed_code, "result_summary": result_summary, "error": error,
            "explanation": explanation,
        }, timeout=5)
    except Exception:
        pass


def render_result(result: dict):
    if result is None:
        return

    rtype = result.get("result_type", "text")
    data = result.get("data", "")
    stdout = result.get("stdout", "")

    if stdout:
        with st.expander("🖥️ Console Output", expanded=False):
            st.code(stdout, language="text")

    if rtype == "chart":
        if isinstance(data, list):
            cols = st.columns(min(len(data), 2))
            for i, b64 in enumerate(data):
                with cols[i % 2]:
                    st.image(base64.b64decode(b64), use_container_width=True)
        else:
            st.image(base64.b64decode(data), use_container_width=True)

    elif rtype == "dataframe":
        try:
            df_result = pd.read_json(data, orient="records")
            st.dataframe(df_result, use_container_width=True, height=400)
            st.caption(f"📏 {len(df_result)} dòng × {len(df_result.columns)} cột")
        except Exception:
            st.code(data)

    elif rtype == "text":
        st.success(data)

    elif rtype == "error":
        st.error(data)


# =============================================
# SIDEBAR
# =============================================
with st.sidebar:
    st.title("🏠 AI Assistant")

    # ── Backend status ──
    is_online = check_backend()
    if is_online:
        st.caption("🟢 Backend hoạt động")

        try:
            schema_resp = requests.get(f"{BACKEND_URL}/api/schema", timeout=3)
            if schema_resp.status_code == 200:
                schema = schema_resp.json()

                c1, c2 = st.columns(2)
                c1.metric("Số dòng", f"{schema['total_rows']:,}")
                c2.metric("Số cột", schema["total_columns"])

                with st.expander("📋 Danh sách cột"):
                    for ci in schema["columns"]:
                        is_num = "int" in ci["dtype"] or "float" in ci["dtype"]
                        icon = "🔢" if is_num else "📝"
                        st.markdown(
                            f"{icon} **{ci['name']}** · `{ci['dtype']}`  \n"
                            f"&nbsp;&nbsp;&nbsp;_{ci['null_percent']}% null · {ci['unique_count']} unique_"
                        )
        except Exception:
            pass
    else:
        st.caption("🔴 Backend offline")
        st.code("python -m uvicorn AI_App.backend.api:app --reload --port 8000", language="bash")

    st.divider()

    # ── Gợi ý câu hỏi ──
    st.subheader("💡 Gợi ý")
    suggestions = [
        "Phân bố giá BĐS theo thành phố",
        "Top 10 quận có giá trung bình cao nhất",
        "Mối tương quan giữa diện tích và giá",
        "Phân tích xu hướng giá theo thời gian",
        "So sánh giá BĐS có vs không thang máy",
    ]
    for s in suggestions:
        if st.button(f"💬 {s}", key=f"sug_{hash(s)}", use_container_width=True):
            st.session_state.last_prompt = s
            st.rerun()

    st.divider()

    if st.button("🗑️ Xóa lịch sử chat", use_container_width=True):
        for k in ["messages", "pending_code", "explanation", "original_code", "last_result", "pending_parameters"]:
            st.session_state[k] = [] if k in ["messages", "pending_parameters"] else None
        st.rerun()


# =============================================
# MAIN CONTENT
# =============================================
st.title("🏠 AI Assistant – Phân tích BĐS Việt Nam")
st.caption("Đặt câu hỏi bằng tiếng Việt → AI sinh code → Bạn review & chỉnh sửa → Approve & Execute")
st.divider()

# ── Chat history ──
for msg in st.session_state.messages:
    role = msg["role"]
    if role == "user":
        with st.chat_message("user", avatar="🧑"):
            st.write(msg["content"])
    elif role == "assistant":
        with st.chat_message("assistant", avatar="🤖"):
            st.write(msg["content"])
            if "explanation" in msg and msg["explanation"]:
                with st.expander("🤖 Giải thích từ AI", expanded=False):
                    st.info(msg["explanation"])
            if "code" in msg:
                with st.expander("📝 Xem code đã chạy", expanded=False):
                    st.code(msg["code"], language="python")
    elif role == "result":
        with st.chat_message("assistant", avatar="📊"):
            render_result(msg["content"])


# =============================================
# CODE EDITOR & APPROVE
# =============================================
if st.session_state.pending_code is not None:
    st.divider()

    # ── Status badge ──
    st.warning("⏳ **PENDING** — Code đang chờ phê duyệt", icon="⏳")

    # ── Giải thích từ AI ──
    if st.session_state.explanation:
        with st.expander("🤖 Giải thích từ AI", expanded=True):
            st.info(st.session_state.explanation)

    # ── Tham số tùy chỉnh ──
    if st.session_state.pending_parameters:
        st.subheader("🎛️ Tùy chỉnh tham số")
        cols = st.columns(min(3, len(st.session_state.pending_parameters)))
        for i, p in enumerate(st.session_state.pending_parameters):
            with cols[i % len(cols)]:
                label = p.get("label", p["name"])
                val = p.get("value", "")
                options = p.get("options")
                
                if options and isinstance(options, list) and len(options) > 0:
                    try:
                        default_idx = options.index(val) if val in options else 0
                    except ValueError:
                        default_idx = 0
                        
                    is_color = "color" in p["name"].lower() or "màu" in label.lower()
                    def color_format(opt):
                        if not is_color: return opt
                        opt_lower = str(opt).lower()
                        color_emojis = {
                            "blue": "🔵", "xanh dương": "🔵", "xanh biển": "🔵",
                            "green": "🟢", "xanh lá": "🟢",
                            "red": "🔴", "đỏ": "🔴",
                            "purple": "🟣", "tím": "🟣",
                            "orange": "🟠", "cam": "🟠",
                            "yellow": "🟡", "vàng": "🟡",
                            "black": "⚫", "đen": "⚫",
                            "white": "⚪", "trắng": "⚪",
                            "brown": "🟤", "nâu": "🟤",
                            "cyan": "🩵", "pink": "🩷", "hồng": "🩷"
                        }
                        return f"{color_emojis.get(opt_lower, '🎨')} {opt}"

                    p["value"] = st.selectbox(
                        label, 
                        options, 
                        index=default_idx, 
                        key=f"param_{p['name']}",
                        format_func=color_format
                    )
                elif p.get("type") == "int":
                    p["value"] = st.number_input(label, value=int(val) if val else 0, key=f"param_{p['name']}")
                elif p.get("type") == "float":
                    p["value"] = st.number_input(label, value=float(val) if val else 0.0, key=f"param_{p['name']}")
                else:
                    p["value"] = st.text_input(label, value=str(val), key=f"param_{p['name']}")

    # ── Code editor ──
    with st.expander("✏️ Xem code nguồn", expanded=not bool(st.session_state.pending_parameters)):
        st.code(st.session_state.pending_code, language="python")
        edited_code = st.session_state.pending_code

    # ── Action buttons ──
    col1, col2, _ = st.columns([2, 2, 6])
    with col1:
        approve = st.button("✅ Approve & Execute", type="primary", use_container_width=True)
    with col2:
        reject = st.button("❌ Hủy", use_container_width=True)

    if approve:
        with st.spinner("⚙️ Đang thực thi..."):
            # Inject parameters into code
            param_injections = ""
            if st.session_state.pending_parameters:
                for p in st.session_state.pending_parameters:
                    if p.get("type") in ["int", "float"]:
                        param_injections += f"{p['name']} = {p['value']}\n"
                    else:
                        param_injections += f"{p['name']} = {repr(str(p['value']))}\n"
                
            final_code_to_execute = param_injections + "\n" + edited_code

            result = call_execute(final_code_to_execute)

        if result:
            st.session_state.last_result = result
            icon = "✅" if result["success"] else "❌"
            st.session_state.messages.append({
                "role": "assistant",
                "content": f"{icon} Code đã thực thi ({result['result_type']})",
                "code": final_code_to_execute,
                "explanation": st.session_state.explanation
            })
            st.session_state.messages.append({"role": "result", "content": result})

            call_log(
                user_prompt=st.session_state.last_prompt or "",
                generated_code=st.session_state.original_code or "",
                executed_code=final_code_to_execute,
                result_summary=result.get("data", "")[:300] if isinstance(result.get("data"), str) else "chart/dataframe",
                error=result.get("data", "") if not result["success"] else "",
                explanation=st.session_state.explanation or "",
            )

        st.session_state.pending_code = None
        st.session_state.explanation = None
        st.session_state.original_code = None
        st.session_state.pending_parameters = []
        st.rerun()

    if reject:
        st.session_state.pending_code = None
        st.session_state.explanation = None
        st.session_state.original_code = None
        st.session_state.pending_parameters = []
        st.session_state.messages.append({"role": "assistant", "content": "🚫 Code đã bị hủy."})
        st.rerun()


# =============================================
# CHAT INPUT
# =============================================
if st.session_state.last_prompt and st.session_state.pending_code is None:
    prompt = st.session_state.last_prompt
    st.session_state.last_prompt = None

    st.session_state.messages.append({"role": "user", "content": prompt})

    with st.spinner("🤖 AI đang phân tích và sinh code..."):
        result = call_generate(prompt)

    if result and result.get("code"):
        st.session_state.pending_code = result["code"]
        st.session_state.explanation = result.get("explanation", "")
        st.session_state.original_code = result["code"]
        st.session_state.pending_parameters = result.get("parameters", [])
        st.session_state.messages.append({
            "role": "assistant",
            "content": "📝 Đã sinh code. Vui lòng review và nhấn **Approve & Execute** để chạy."
        })
    elif result:
        if result.get("error"):
            st.session_state.messages.append({
                "role": "assistant",
                "content": f"⚠️ Lỗi: {result.get('explanation', 'Unknown error')}"
            })
        else:
            # AI chỉ trả lời bằng chữ (tư vấn, gợi ý)
            st.session_state.messages.append({
                "role": "assistant",
                "content": result.get("explanation", "")
            })
    st.rerun()

# ── Chat input ──
user_input = st.chat_input("💬 Đặt câu hỏi phân tích dữ liệu bất động sản...")
if user_input:
    if st.session_state.pending_code is not None:
        st.warning("⚠️ Hãy Approve hoặc Hủy code hiện tại trước.")
    else:
        st.session_state.messages.append({"role": "user", "content": user_input})
        st.session_state.last_prompt = user_input
        st.rerun()
