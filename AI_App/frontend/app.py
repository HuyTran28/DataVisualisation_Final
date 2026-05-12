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
import uuid
import os
import time
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
    #MainMenu, footer {visibility: hidden;}
    header {background: transparent;}

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
    
    /* ── Custom User Chat Message ── */
    .user-msg {
        display: flex;
        justify-content: flex-end;
        align-items: flex-start;
        margin-bottom: 1rem;
        gap: 10px;
    }
    .user-bubble {
        background-color: #0068c9;
        color: white;
        padding: 0.75rem 1.25rem;
        border-radius: 1.5rem 1.5rem 0 1.5rem;
        max-width: 80%;
        line-height: 1.5;
        box-shadow: 0 2px 5px rgba(0,0,0,0.1);
    }
    .user-avatar {
        font-size: 1.8rem;
        margin-top: -0.2rem;
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
        "current_chat_id": str(uuid.uuid4()),
    }
    for key, val in defaults.items():
        if key not in st.session_state:
            st.session_state[key] = val

init_session_state()


# =============================================
# HELPER FUNCTIONS
# =============================================
CHAT_FILE = os.path.join(os.path.dirname(__file__), "local_chats.json")

def load_chats():
    if os.path.exists(CHAT_FILE):
        try:
            with open(CHAT_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            return []
    return []

def save_chats(chats):
    # Keep max 10, newest first
    chats = sorted(chats, key=lambda x: x.get("timestamp", 0), reverse=True)[:10]
    with open(CHAT_FILE, "w", encoding="utf-8") as f:
        json.dump(chats, f, ensure_ascii=False, indent=2)

def save_current_chat():
    if not st.session_state.messages:
        return
        
    chats = load_chats()
    
    title = "Đoạn chat mới"
    for m in st.session_state.messages:
        if m["role"] == "user":
            title = m["content"][:35] + ("..." if len(m["content"]) > 35 else "")
            break
            
    chat_id = st.session_state.current_chat_id
    chat_data = {
        "id": chat_id,
        "timestamp": time.time(),
        "title": title,
        "messages": st.session_state.messages
    }
    
    updated = False
    for i, c in enumerate(chats):
        if c["id"] == chat_id:
            chats[i] = chat_data
            updated = True
            break
    if not updated:
        chats.append(chat_data)
        
    save_chats(chats)

def load_chat(chat_id):
    chats = load_chats()
    for c in chats:
        if c["id"] == chat_id:
            st.session_state.messages = c["messages"]
            st.session_state.current_chat_id = chat_id
            st.session_state.pending_code = None
            st.session_state.explanation = None
            st.session_state.original_code = None
            st.session_state.pending_parameters = []
            st.rerun()

def start_new_chat():
    st.session_state.messages = []
    st.session_state.current_chat_id = str(uuid.uuid4())
    st.session_state.pending_code = None
    st.session_state.explanation = None
    st.session_state.original_code = None
    st.session_state.pending_parameters = []
    st.rerun()

def delete_chat(chat_id):
    chats = load_chats()
    chats = [c for c in chats if c["id"] != chat_id]
    save_chats(chats)
    
    if st.session_state.current_chat_id == chat_id:
        start_new_chat()
    else:
        st.rerun()

@st.cache_data(ttl=10)
def check_backend() -> bool:
    try:
        r = requests.get(f"{BACKEND_URL}/api/health", timeout=1)
        return r.status_code == 200
    except Exception:
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

        if "schema_data" not in st.session_state:
            with st.status("📥 Đang đọc schema dữ liệu...", expanded=True) as status:
                st.write("Đang gọi API lấy schema từ backend...")
                try:
                    schema_resp = requests.get(f"{BACKEND_URL}/api/schema", timeout=5)
                    if schema_resp.status_code == 200:
                        st.session_state.schema_data = schema_resp.json()
                        status.update(label="✅ Đã đọc schema!", state="complete", expanded=False)
                    else:
                        status.update(label="❌ Lỗi đọc schema!", state="error", expanded=False)
                except Exception:
                    status.update(label="❌ Không kết nối được API!", state="error", expanded=False)

        if "schema_data" in st.session_state:
            schema = st.session_state.schema_data
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
    else:
        st.caption("🔴 Backend offline")
        st.code("python -m uvicorn AI_App.backend.api:app --reload --port 8000", language="bash")

    st.divider()

    # ── Lịch sử Chat (Local) ──
    st.subheader("📚 Lịch sử Chat")
    if st.button("➕ Tạo đoạn chat mới", type="primary", use_container_width=True):
        start_new_chat()
        
    past_chats = load_chats()
    if past_chats:
        st.write("")
        for c in past_chats:
            is_current = c["id"] == st.session_state.current_chat_id
            col1, col2 = st.columns([8, 2])
            with col1:
                btn_label = f"📍 {c['title']}" if is_current else f"💬 {c['title']}"
                if st.button(btn_label, key=f"chat_{c['id']}", use_container_width=True):
                    if not is_current:
                        load_chat(c["id"])
            with col2:
                if st.button("❌", key=f"del_{c['id']}", help="Xóa đoạn chat này", use_container_width=True):
                    delete_chat(c["id"])
    else:
        st.caption("Chưa có đoạn chat nào được lưu.")

    st.divider()

    if st.button("🗑️ Xóa tất cả lịch sử", use_container_width=True):
        if os.path.exists(CHAT_FILE):
            os.remove(CHAT_FILE)
        start_new_chat()


# =============================================
# MAIN CONTENT
# =============================================
st.title("🏠 AI Assistant – Phân tích BĐS Việt Nam")
st.caption("Đặt câu hỏi bằng tiếng Việt → AI sinh code → Bạn review & chỉnh sửa → Approve & Execute")
st.divider()

# ── Chat history ──
chat_container = st.container()

with chat_container:
    for msg in st.session_state.messages:
        role = msg["role"]
        if role == "user":
            html = f"""
            <div class="user-msg">
                <div class="user-bubble">{msg['content']}</div>
                <div class="user-avatar">🧑</div>
            </div>
            """
            st.markdown(html, unsafe_allow_html=True)
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
                
                if p.get("type") == "list":
                    default_vals = val if isinstance(val, list) else []
                    opts = options if options and isinstance(options, list) else default_vals
                    default_vals = [v for v in default_vals if v in opts]
                    p["value"] = st.multiselect(
                        label,
                        options=opts,
                        default=default_vals,
                        key=f"param_{p['name']}"
                    )
                elif options and isinstance(options, list) and len(options) > 0:
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

    # ── Reprompt AI ──
    st.markdown("---")
    st.subheader("🔄 Yêu cầu AI sửa lại code")
    reprompt_text = st.text_input("Nhập yêu cầu sửa code:", placeholder="Ví dụ: Đổi biểu đồ thành màu đỏ, thêm tiêu đề...")

    # ── Action buttons ──
    # Check if we just got an error
    has_error = st.session_state.last_result and not st.session_state.last_result.get("success")
    
    if has_error:
        col1, col2, col3, col4, _ = st.columns([2.5, 2, 2.5, 2.5, 2])
        with col1:
            approve = st.button("✅ Approve & Execute", type="primary", use_container_width=True)
        with col3:
            reprompt_btn = st.button("🔄 Gửi yêu cầu sửa", use_container_width=True)
        with col4:
            auto_fix = st.button("🛠️ Tự động sửa lỗi", type="primary", use_container_width=True)
        with col2:
            reject = st.button("❌ Hủy", use_container_width=True)
    else:
        col1, col2, col3, _ = st.columns([2.5, 2, 2.5, 3])
        with col1:
            approve = st.button("✅ Approve & Execute", type="primary", use_container_width=True)
        with col3:
            reprompt_btn = st.button("🔄 Gửi yêu cầu sửa", use_container_width=True)
        with col2:
            reject = st.button("❌ Hủy", use_container_width=True)
        auto_fix = False

    if (reprompt_btn and reprompt_text.strip()) or auto_fix:
        # Lấy tham số hiện tại để AI biết
        param_injections = ""
        if st.session_state.pending_parameters:
            for p in st.session_state.pending_parameters:
                if p.get("type") in ["int", "float", "list"]:
                    param_injections += f"{p['name']} = {p['value']}\n"
                else:
                    param_injections += f"{p['name']} = {repr(str(p['value']))}\n"
            
        current_code = param_injections + "\n" + edited_code
        
        if auto_fix:
            error_data = st.session_state.last_result.get("data", "Unknown error")
            reprompt_text = f"Fix lỗi runtime sau:\n{error_data}"
            
        combined_prompt = (
            f"Code hiện tại đang là:\n```python\n{current_code}\n```\n\n"
            f"Yêu cầu sửa lại: {reprompt_text}\n\n"
            "LƯU Ý TRỌNG YẾU:\n"
            "1. TUYỆT ĐỐI không để xảy ra lỗi cú pháp (SyntaxError). Đặc biệt chú ý khi dùng f-string: nếu bên trong có dấu nháy đơn `'` (như `', '.join()`), hãy dùng dấu nháy kép `\"` bao quanh f-string bên ngoài (ví dụ: `f\"... {', '.join(...)} ...\"`).\n"
            "2. Nếu có tham số là danh sách (list), hãy đảm bảo định nghĩa `\"type\": \"list\"` và `value` phải là một mảng thực sự (ví dụ `[\"A\", \"B\"]`), TUYỆT ĐỐI KHÔNG dùng chuỗi string `\"['A', 'B']\"`.\n"
            "3. Trả về đúng format JSON quy định."
        )
        
        st.session_state.messages.append({"role": "user", "content": f"Yêu cầu sửa code: {reprompt_text}"})
        
        with st.status("🤖 Đang sửa code...", expanded=True) as status:
            st.write("⏳ Đang phân tích yêu cầu sửa...")
            st.write("📡 Đang gọi API xử lý từ LLM...")
            result = call_generate(combined_prompt)
            if result:
                status.update(label="✅ Đã sửa xong!", state="complete", expanded=False)
            else:
                status.update(label="❌ Lỗi xử lý!", state="error", expanded=False)
        
        if result and result.get("code"):
            st.session_state.pending_code = result["code"]
            st.session_state.explanation = result.get("explanation", "")
            st.session_state.original_code = result["code"]
            st.session_state.pending_parameters = result.get("parameters", [])
            st.session_state.messages.append({
                "role": "assistant",
                "content": "📝 Đã sửa code theo yêu cầu. Vui lòng review."
            })
        elif result:
            if result.get("error"):
                st.session_state.messages.append({
                    "role": "assistant",
                    "content": f"⚠️ Lỗi: {result.get('explanation', 'Unknown error')}"
                })
            else:
                st.session_state.messages.append({
                    "role": "assistant",
                    "content": result.get("explanation", "Lỗi khi sửa code.")
                })
        
        save_current_chat()
        st.rerun()

    elif approve:
        with st.status("⚙️ Đang thực thi code...", expanded=True) as status:
            st.write("⏳ Đang khởi tạo môi trường an toàn...")
            st.write("🚀 Đang chạy code phân tích...")
            # Inject parameters into code
            param_injections = ""
            if st.session_state.pending_parameters:
                for p in st.session_state.pending_parameters:
                    if p.get("type") in ["int", "float", "list"]:
                        param_injections += f"{p['name']} = {p['value']}\n"
                    else:
                        param_injections += f"{p['name']} = {repr(str(p['value']))}\n"
                
            final_code_to_execute = param_injections + "\n" + edited_code

            result = call_execute(final_code_to_execute)
            if result and result.get("success"):
                status.update(label="✅ Thực thi thành công!", state="complete", expanded=False)
            else:
                status.update(label="❌ Lỗi thực thi!", state="error", expanded=False)

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

        if result and result.get("success"):
            st.session_state.pending_code = None
            st.session_state.explanation = None
            st.session_state.original_code = None
            st.session_state.pending_parameters = []
            st.session_state.last_result = None
            
        save_current_chat()
        st.rerun()

    if reject:
        st.session_state.pending_code = None
        st.session_state.explanation = None
        st.session_state.original_code = None
        st.session_state.pending_parameters = []
        st.session_state.last_result = None
        st.session_state.messages.append({"role": "assistant", "content": "🚫 Code đã bị hủy."})
        st.rerun()


# =============================================
# CHAT INPUT & PROCESSING
# =============================================
def process_prompt(prompt):
    if st.session_state.pending_code is not None:
        st.warning("⚠️ Hãy Approve hoặc Hủy code hiện tại trước.")
        return

    st.session_state.messages.append({"role": "user", "content": prompt})
    
    with chat_container:
        html = f"""
        <div class="user-msg">
            <div class="user-bubble">{prompt}</div>
            <div class="user-avatar">🧑</div>
        </div>
        """
        st.markdown(html, unsafe_allow_html=True)
        
        with st.chat_message("assistant", avatar="🤖"):
            with st.status("🤖 Đang phân tích yêu cầu...", expanded=True) as status:
                st.write("⏳ Đang chuẩn bị dữ liệu...")
                st.write("📡 Đang gọi API xử lý từ LLM...")
                result = call_generate(prompt)
                if result:
                    status.update(label="✅ Hoàn tất!", state="complete", expanded=False)
                else:
                    status.update(label="❌ Lỗi xử lý!", state="error", expanded=False)

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
            st.session_state.messages.append({
                "role": "assistant",
                "content": result.get("explanation", "")
            })
            
    save_current_chat()
    st.rerun()

if st.session_state.last_prompt:
    prompt = st.session_state.last_prompt
    st.session_state.last_prompt = None
    process_prompt(prompt)

if not st.session_state.messages:
    st.markdown("### 💡 Gợi ý câu hỏi")
    suggestions = [
        "Phân bố giá BĐS theo thành phố",
        "Top 10 quận có giá trung bình cao nhất",
        "Mối tương quan giữa diện tích và giá",
        "Phân tích xu hướng giá theo thời gian",
        "So sánh giá BĐS có vs không thang máy",
    ]
    col1, col2 = st.columns(2)
    for i, s in enumerate(suggestions):
        with col1 if i % 2 == 0 else col2:
            if st.button(f"💬 {s}", key=f"sug_main_{hash(s)}", use_container_width=True):
                st.session_state.last_prompt = s
                st.rerun()

user_input = st.chat_input("💬 Đặt câu hỏi phân tích dữ liệu bất động sản...")
if user_input:
    process_prompt(user_input)
