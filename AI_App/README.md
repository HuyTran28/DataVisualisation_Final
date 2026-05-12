# 🏠 AI Assistant – Phân tích Bất Động Sản Việt Nam

Đây là module trợ lý AI tích hợp trong hệ thống phân tích dữ liệu bất động sản. Ứng dụng này cho phép người dùng đặt các câu hỏi phân tích bằng ngôn ngữ tự nhiên, sau đó AI sẽ tự động sinh code Python, tính toán và vẽ biểu đồ kết quả.

## 🌟 Tính năng nổi bật
- **Phân tích tự động:** Đặt câu hỏi bằng tiếng Việt (ví dụ: *"Top 10 quận có giá trung bình cao nhất"*). AI sẽ tự hiểu và viết code.
- **Tùy chỉnh tham số trực quan:** Thay vì phải sửa code thô, AI sẽ trích xuất các tham số (tiêu đề, màu sắc, số lượng...) thành các thanh nhập liệu và dropdown chọn màu trên giao diện để người dùng tinh chỉnh.
- **Bảo mật (Human-in-the-loop):** Code do AI sinh ra không được chạy tự động. Người dùng được xem trước, tinh chỉnh tham số và quyết định *"Approve & Execute"* (Phê duyệt và Thực thi). Code chạy trong môi trường an toàn.
- **Lưu vết (Logging):** Mọi phiên tương tác (câu hỏi, code được sinh ra, code đã chạy và giải thích của AI) đều được lưu log chi tiết vào file `backend/logs.json`.

## 🏗 Cấu trúc hệ thống
Hệ thống được chia thành 2 phần độc lập:
1. **Backend (FastAPI):** Giao tiếp với LLM (Google Gemini hoặc Grok), đóng vai trò phân tích yêu cầu, truyền schema dataset, và thực thi code Python trong một môi trường được giới hạn.
2. **Frontend (Streamlit):** Cung cấp giao diện chat thân thiện với người dùng, hỗ trợ hiển thị biểu đồ, bảng dữ liệu, và giao diện điều chỉnh tham số sinh động.

---

## 🚀 Hướng dẫn cài đặt

### 1. Cài đặt các thư viện phụ thuộc
Mở Terminal tại thư mục gốc của project (nơi chứa thư mục `AI_App`) và chạy lệnh sau:
```bash
pip install -r AI_App/requirements.txt
```

### 2. Thiết lập API Key
Trong thư mục `AI_App/`, hãy tạo một file tên là `.env` (nếu chưa có) và thêm các API Key của bạn vào:
```env
# Mặc định sử dụng Gemini, hãy điền Key của bạn vào đây:
GEMINI_API_KEY=your_gemini_api_key_here

# Nếu sử dụng Grok, hãy điền Key và đổi LLM_PROVIDER:
# GROK_API_KEY=your_grok_api_key_here
# LLM_PROVIDER=gemini  # hoặc grok
```

---

## 🏃 Hướng dẫn chạy ứng dụng

Để chạy được toàn bộ ứng dụng, bạn cần bật **2 Terminal riêng biệt**. Đảm bảo cả hai Terminal đều đang đứng ở thư mục gốc của project (thư mục chứa `AI_App/`).

### Terminal 1: Chạy Backend
Chạy lệnh khởi động FastAPI (chạy ở cổng 8000 mặc định):
```bash
python -m uvicorn AI_App.backend.api:app --reload --port 8000
```
*Lưu ý: Đợi Terminal báo `🚀 Backend is ready!` thì chuyển sang bước tiếp theo.*

### Terminal 2: Chạy Frontend
Chạy lệnh khởi động Streamlit:
```bash
streamlit run AI_App/frontend/app.py
```
Sau khi chạy, giao diện sẽ tự động mở lên trên trình duyệt của bạn (thường ở địa chỉ `http://localhost:8501`). Tại thanh bên (sidebar), nếu thấy chữ **🟢 Backend hoạt động** là bạn đã có thể bắt đầu chat với AI.

---

## 🛠 Khắc phục lỗi thường gặp
- **Frontend báo `🔴 Backend offline`**: Kiểm tra lại Terminal 1 xem FastAPI đã chạy thành công chưa, hoặc có bị lỗi API Key không.
- **AI không sinh ra bảng tuỳ chỉnh tham số**: Hãy thử xóa lịch sử chat và đặt lại câu hỏi rõ ràng hơn để AI hiểu được các tham số như "số lượng", "màu sắc", "tên cột" cần được tách riêng.
- **Lỗi `ModuleNotFoundError`**: Đảm bảo bạn đang chạy lệnh uvicorn / streamlit ở **thư mục gốc của project**, không phải ở bên trong thư mục `AI_App` để trình thông dịch Python hiểu được module `AI_App.backend...`.
