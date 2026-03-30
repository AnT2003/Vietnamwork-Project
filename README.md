# 🚀 VietnamWorks AI Headhunter & Data Pipeline

![Python](https://img.shields.io/badge/Python-3.9+-blue.svg)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-pgvector-336791.svg)
![Docker](https://img.shields.io/badge/Docker-Enabled-2496ED.svg)
![Flask](https://img.shields.io/badge/Flask-Web%20App-000000.svg)
![Ollama](https://img.shields.io/badge/AI-Ollama%20LLM-white.svg)

Hệ thống **AI Headhunter** thông minh kết hợp **Data Engineering Pipeline** toàn diện. Dự án tự động thu thập dữ liệu việc làm từ VietnamWorks, xử lý ETL/ELT, lưu trữ vào Data Warehouse & Vector Database, đồng thời ứng dụng công nghệ **RAG (Retrieval-Augmented Generation)** kết hợp **Hybrid Search** để phân tích CV và tư vấn công việc tối ưu nhất cho ứng viên.

---

## 🌟 Tính năng nổi bật (Key Features)

- **🔄 Automated Data Pipeline (ETL/ELT):** Xây dựng luồng xử lý dữ liệu tự động từ khâu thu thập (Crawler), làm sạch (Clean), chuẩn hóa và nạp vào Data Warehouse (PostgreSQL) qua các phân lớp: `Staging` -> `DWH` -> `Vector DWH`. Thiết kế theo mô hình dữ liệu chuẩn (Star Schema).
- **🧠 AI Vectorization:** Ứng dụng mô hình `SentenceTransformers` (`keepitreal/vietnamese-sbert`) để nhúng (embed) các mô tả công việc thành vector đa chiều, tối ưu hóa đặc biệt cho ngữ nghĩa tiếng Việt.
- **🔍 Hybrid Search (RRF):** Kết hợp sức mạnh của tìm kiếm ngữ nghĩa (Semantic/Vector Search) và tìm kiếm từ khóa (Lexical Search). Áp dụng thuật toán **Reciprocal Rank Fusion (RRF)** giúp truy xuất công việc chuẩn xác dựa trên Intent (ý định) của người dùng.
- **📄 CV Parsing & RAG LLM:** Tự động trích xuất thông tin từ CV (PDF). Tích hợp LLM (`gpt-oss:120b-cloud` qua Ollama) để đối chiếu, so sánh điểm mạnh/yếu của ứng viên với từng vị trí công việc. Hệ thống có khả năng tự động render định dạng HTML Table cho bảng so sánh đẹp mắt.
- **💻 Modern Web Interface:** Giao diện Dashboard thống kê trực quan và hệ thống Chatbot AI mượt mà, xử lý hiển thị Markdown và HTML Table thời gian thực.

---

## 🛠️ Công nghệ sử dụng (Tech Stack)

### 1. Data Engineering & Database
- **Data Pipeline:** Python, Pandas, SQLAlchemy, psycopg2.
- **Database:** PostgreSQL (tích hợp extension `pgvector` cho Vector Storage).
- **Data Architecture:** Data Warehouse, Data Modeling.

### 2. AI & Machine Learning
- **Embedding Model:** `sentence-transformers` (Vietnamese SBERT).
- **LLM Engine:** Ollama API (`gpt-oss:120b-cloud`).
- **NLP & Xử lý văn bản:** Pyvi (Vietnamese Tokenizer), PyPDF2, Regex (Auto-fix HTML/Markdown Tables).

### 3. Backend & Frontend
- **Backend:** Python / Flask.
- **Frontend:** HTML5, TailwindCSS, FontAwesome, `marked.js` (dịch Markdown siêu tốc trên Client-side).

### 4. DevOps & Deployment
- **Containerization:** Docker & Docker Compose.

---

## 📂 Kiến trúc hệ thống (System Architecture)

```text
[VietnamWorks Website] --(Crawler)--> [Raw CSVs] 
                                          |
                                    (ETL Pipeline)
                                          v
                                [PostgreSQL Staging]
                                          |
                                  (Transform & Load)
                                          v
                            [PostgreSQL Data Warehouse] 
                                          |
                                 (AI Vectorization)
                                          v
                                 [pgvector Database]
                                          |
[User Uploads CV] -> [Intent Parser] -> [Hybrid Search (RRF)] <----+
                                          |                        |
                                 [Top K Matched Jobs]              |
                                          |                        |
                                  [Ollama LLM (RAG)]---------------+
                                          |
                              [Personalized AI Response]
```

---

## 📁 Cấu trúc thư mục (Project Structure)

```text
Vietnamworks_Project/
│
├── core_engine/              # Lõi xử lý AI và Data Access
│   ├── ai_engine.py          # Logic Hybrid Search, Vector Embedding, LLM RAG
│   └── dashboard_engine.py   # Xử lý truy xuất dữ liệu cho Dashboard
│
├── dags/                     # Data Pipeline & ETL Scripts
│   ├── scripts/
│   │   ├── ai_tasks.py       # Tác vụ NLP & Vectorization (Sinh và nạp Vector)
│   │   ├── crawl_vietnamworks.py # Crawler thu thập data gốc
│   │   ├── sql_queries.py    # Chứa các câu lệnh SQL (DDL/DML)
│   │   └── logger.py         # Ghi log tiến trình Pipeline
│   └── initial_load.py       # Script khởi chạy toàn bộ chu trình ETL (End-to-End)
│
├── data/raw/                 # Không gian lưu trữ dữ liệu thô (Raw CSV)
├── sql/                      # Chứa script khởi tạo Data Warehouse (setup_database.sql)
├── templates/                # Giao diện Frontend (HTML/TailwindCSS)
│   ├── chat.html             # Giao diện hệ thống AI Headhunter
│   └── dashboard.html        # Giao diện thống kê thị trường
│
├── app.py                    # Điểm đầu vào của Web App (Flask Routing, API & Auto-Fix HTML)
├── config.py                 # File cấu hình biến môi trường, Database URI, LLM Models
├── docker-compose.yml        # Cấu hình dịch vụ Docker (App, Postgres)
├── Dockerfile                # Cấu hình build image cho môi trường Python
└── requirements.txt          # Danh sách thư viện phụ thuộc
```

---

## 🚀 Hướng dẫn Cài đặt & Khởi chạy (Getting Started)

### Yêu cầu hệ thống (Prerequisites)
- Đã cài đặt [Docker](https://www.docker.com/) và Docker Compose.
- Môi trường cấp phát RAM tối thiểu 4GB-8GB cho Docker.
- API Key của Ollama (Nếu dùng LLM Cloud) hoặc cài đặt Ollama Local.

### Bước 1: Clone Repository
```bash
git clone [https://github.com/your-username/Vietnamworks_Project.git](https://github.com/your-username/Vietnamworks_Project.git)
cd Vietnamworks_Project
```

### Bước 2: Cấu hình biến môi trường
Tạo file `.env` ở thư mục gốc của dự án và điền các thông tin sau:
```env
DB_URI=postgresql://user:password@postgres_db:5432/vietnamworks_db
OLLAMA_API_KEY=your_api_key_here
```

### Bước 3: Build và chạy hệ thống bằng Docker
Khởi chạy các container nền tảng (Database và Web App):
```bash
docker-compose up --build -d
```

### Bước 4: Khởi tạo dữ liệu (Initial Load)
Chạy lệnh sau để kích hoạt toàn bộ luồng Big Data Pipeline (Crawler -> ETL -> NLP Transform -> Vectorization):
```bash
docker exec -it vww_flask_app python dags/initial_load.py
```
*(Lưu ý: Quá trình này sẽ mất một khoảng thời gian tùy thuộc vào số lượng Jobs được thu thập và tốc độ nhúng Vector của hệ thống phần cứng).*

---

## 🎯 Hướng dẫn Sử dụng (Usage)

1. Mở trình duyệt và truy cập vào địa chỉ: `http://localhost:6868`
2. **Thống kê thị trường (Dashboard):** Xem các biểu đồ trực quan, phân tích xu hướng tuyển dụng, mức lương trung bình theo ngành nghề và cấp bậc.
3. **AI Assistant (AI Headhunter):**
   - Chuyển sang tab **AI Assistant**.
   - Bấm vào biểu tượng 📎 (Ghim) để **Upload CV (định dạng PDF)** của bạn.
   - Nhập yêu cầu, ví dụ: *"Hãy phân tích CV của tôi và tìm vị trí Data Engineer hoặc Data Analyst phù hợp nhất tại Hà Nội"*.
   - Hệ thống RAG sẽ quét Data Warehouse, đánh giá mức độ phù hợp giữa kỹ năng trong CV và yêu cầu công việc, sau đó trả về danh sách phân tích chi tiết (có hỗ trợ hiển thị dưới dạng bảng biểu HTML rõ ràng) kèm Link ứng tuyển trực tiếp.

---

## 🔮 Hướng phát triển tương lai (Future Enhancements)

- [ ] Tích hợp **Apache Airflow** để lập lịch (Scheduling) và giám sát luồng dữ liệu tự động hàng ngày thay vì chạy script thủ công.
- [ ] Mở rộng Data Lake trên nền tảng Cloud (GCP/AWS) để xử lý lượng dữ liệu lớn hơn.
- [ ] Kết nối thêm các nguồn dữ liệu từ nền tảng khác để làm phong phú Data Warehouse.
- [ ] Phát triển tính năng Agentic AI tự động sinh Cover Letter dựa trên Job Description.

---

## 👨‍💻 Tác giả (Author)

**[Thái Bảo An]** *Data Engineer*
- 🌐 **Portfolio / GitHub:** [Link GitHub của bạn]

*Nếu bạn thấy dự án này thú vị và hữu ích, đừng quên để lại 1 ⭐️ cho repository nhé!*
