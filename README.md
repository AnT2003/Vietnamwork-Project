# 🚀 VietnamWorks AI Headhunter & Data Pipeline

![Python](https://img.shields.io/badge/Python-3.9+-blue.svg)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-pgvector-336791.svg)
![Docker](https://img.shields.io/badge/Docker-Enabled-2496ED.svg)
![Apache Airflow](https://img.shields.io/badge/Apache%20Airflow-2.8.1-017CEE.svg)
![Flask](https://img.shields.io/badge/Flask-Web%20App-000000.svg)
![Ollama](https://img.shields.io/badge/AI-Ollama%20LLM-white.svg)

Hệ thống **AI Headhunter** thông minh kết hợp **Data Engineering Pipeline** toàn diện (End-to-End). Dự án giải quyết bài toán từ khâu tự động thu thập dữ liệu việc làm trên thị trường, xử lý chuẩn hóa ETL/ELT, xây dựng mô hình dữ liệu tối ưu trong Data Warehouse, cho đến việc ứng dụng công nghệ **RAG (Retrieval-Augmented Generation)** kết hợp **Hybrid Search** để phân tích CV và tư vấn định hướng nghề nghiệp cho ứng viên.

---

## 🌟 Tính năng nổi bật (Key Features)

- **🔄 Automated Data Pipeline (ETL/ELT):** Vận hành luồng xử lý dữ liệu hoàn toàn tự động từ khâu thu thập (Crawler), làm sạch (Clean) và nạp vào kho dữ liệu qua các phân lớp: `Raw` -> `Staging` -> `Data Warehouse` -> `Vector DWH`. 
- **🛡️ Data Quality Framework:** Tích hợp bộ quy tắc kiểm định dữ liệu nghiêm ngặt (Data Validation) ngay trong Pipeline để bắt lỗi dữ liệu rác (Khóa chính NULL, mức lương âm, trùng lặp ID, vi phạm logic ngày tháng) trước khi nạp vào DWH.
- **🧠 AI Vectorization:** Ứng dụng mô hình `SentenceTransformers` (`keepitreal/vietnamese-sbert`) để nhúng (embed) các mô tả công việc thành vector đa chiều, tối ưu hóa đặc biệt cho ngữ nghĩa tiếng Việt.
- **🔍 Hybrid Search (RRF):** Kết hợp sức mạnh của tìm kiếm ngữ nghĩa (Semantic/Vector Search) và tìm kiếm từ khóa (Lexical Search) bằng thuật toán **Reciprocal Rank Fusion (RRF)**, giúp truy xuất công việc chuẩn xác theo Intent của người dùng.
- **📄 CV Parsing & RAG LLM:** Tự động trích xuất thông tin từ CV (PDF). Tích hợp LLM (`gpt-oss:120b-cloud` qua Ollama) để đối chiếu, so sánh điểm mạnh/yếu của ứng viên với từng vị trí công việc và tự động render định dạng HTML Table.
- **⏰ Tự động hóa với Airflow:** Lập lịch Delta Load hằng ngày, đảm bảo dữ liệu thị trường luôn được cập nhật mới nhất mà không cần can thiệp thủ công.

---

## 📊 Nguồn dữ liệu & Mô hình dữ liệu (Data Modeling)

### 1. Giới thiệu Dữ liệu (Data Introduction)
Dữ liệu được thu thập trực tiếp từ nền tảng VietnamWorks, tập trung vào các thông tin cốt lõi của thị trường lao động:
* **Thông tin công việc:** Tiêu đề, cấp bậc, mức lương, địa điểm, ngành nghề.
* **Yêu cầu & Quyền lợi:** Kỹ năng cốt lõi (Skills), số năm kinh nghiệm, mô tả chi tiết (JD), phúc lợi.
* **Thông tin doanh nghiệp:** Tên công ty, quy mô, địa chỉ.

### 2. Mô hình dữ liệu (Star Schema)
Để tối ưu hóa cho các truy vấn phân tích trên Dashboard và tìm kiếm AI, kho dữ liệu (DWH) được thiết kế theo kiến trúc **Star Schema** kinh điển, bao gồm:
* **`fact_job_postings`:** Bảng sự kiện trung tâm, lưu trữ các transaction đăng tuyển (job_id, company_id, date_keys).
* **`dim_job_details`:** Bảng chiều lưu trữ các thuộc tính chi tiết của công việc (salary, level, requirements, benefits).
* **`dim_companies`:** Bảng chiều quản lý thông tin định danh của nhà tuyển dụng.
* **`vector_dwh.dim_job_vectors`:** Phân vùng đặc biệt sử dụng extension `pgvector` để lưu trữ dữ liệu đã mã hóa (Embeddings) phục vụ cho truy vấn Semantic Search.

---

## 🛠️ Công nghệ sử dụng (Tech Stack)

* **Data Engineering:** Python, Pandas, SQLAlchemy, psycopg2, Apache Airflow.
* **Database:** PostgreSQL (tích hợp `pgvector`).
* **AI & NLP:** `sentence-transformers`, Ollama API, Pyvi, PyPDF2.
* **Backend & Frontend:** Flask, HTML5, TailwindCSS, Chart.js.
* **DevOps:** Docker, Docker Compose, Bash Script.

---

## 📂 Kiến trúc hệ thống & Cấu trúc thư mục

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

## Pipeline tổng quan:
<img width="1464" height="378" alt="image" src="https://github.com/user-attachments/assets/7552806e-5403-4677-94a7-78a91ba0ea54" />


---

## 📁 Cấu trúc thư mục (Project Structure)

```text
Vietnamworks_Project/
├── core_engine/              # Lõi xử lý AI (Hybrid Search, LLM RAG) và Data Access
    ├── dashboard_engine.py
    ├── ai_engine.py
├── dags/                     # Thư mục chứa các luồng Apache Airflow
│   ├── scripts/
│   │   ├── ai_tasks.py       # Tác vụ NLP & Vectorization
│   │   ├── crawl_vietnamworks.py # Crawler thu thập data gốc
│   │   ├── sql_queries.py    # DDL/DML Queries
│   │   └── logger.py         # Ghi log tiến trình Pipeline
│   ├── initial_load.py       # Script nạp dữ liệu khổng lồ lần đầu (Full Load)
│   ├── master_pipeline.py    # Script ETL cập nhật dữ liệu hằng ngày (Delta Load)
│   └── daily_scheduler.py    # Cấu hình DAG cho Airflow
├── sql/                      # Script khởi tạo Database Schema
    ├── setup_database.sql  
├── templates/                # Giao diện Web 
    ├── index.html            # Giao diện trang chủ
    ├── dashboard.html        # Giao diện Dashboard
    ├── chat.html             # Giao diện chatbot
├── app.py                    # Flask Web App Server
├── config.py                 # File cấu hình biến môi trường
├── docker-compose.yml        # Cấu hình Docker Services
├── Dockerfile                # Image Build File
├── requirements.txt          # Thư viện phụ thuộc
└── start.sh                  # Kịch bản khởi động tự động (Airflow + Flask)
```

---

## 🚀 Hướng dẫn Cài đặt & Khởi chạy (Getting Started)

### Yêu cầu hệ thống (Prerequisites)
- Đã cài đặt [Docker](https://www.docker.com/) và Docker Compose.
- Lưu ý quan trọng: Đảm bảo Docker Desktop đã được mở và biểu tượng cá voi ở dưới thanh taskbar đang báo "Engine running"
- Môi trường cấp phát RAM tối thiểu 4GB-8GB cho Docker.
- Tạo tài khoản và lấy API Key của Ollama để dùng LLM Cloud tại: https://ollama.com/settings/keys

### Bước 1: Clone Repository
```bash
git clone https://github.com/AnT2003/Vietnamwork-Project.git
cd Vietnamworks_Project
```

### Bước 2: Cấu hình biến môi trường
Tạo file `.env` ở thư mục gốc của dự án và điền các thông tin sau:
```env
DB_URI=postgresql://postgres:password123@vww_postgres_db:5455/postgres
OLLAMA_API_KEY=your_api_key_here
```

### Bước 3: Build và chạy hệ thống bằng Docker
Khởi chạy các container nền tảng (Database và Web App):
```bash
docker-compose up --build -d
```

### Bước 4: Khởi tạo dữ liệu (Initial Load)
Chạy lệnh sau để kích hoạt toàn bộ luồng Big Data Pipeline (Crawler -> ETL -> validation -> NLP Transform -> Vectorization):
```bash
docker exec -it vww_flask_app python dags/initial_load.py
```
*(Lưu ý: Quá trình này sẽ mất một khoảng thời gian tùy thuộc vào số lượng Jobs được thu thập và tốc độ nhúng Vector của hệ thống phần cứng).*

---

### Bước 5: Khởi động pipeline nạp thêm 100 dữ liệu hàng ngày bằng lệnh
Chạy lệnh sau để kích hoạt toàn bộ luồng ETL Pipeline nạp thêm data (Crawler -> ETL ->validation -> NLP Transform -> Vectorization):
```bash
docker exec -it vww_flask_app python dags/master_pipeline.py
```

---

## ⏰ Hướng dẫn cấu hình Lập lịch tự động (Apache Airflow Scheduler)

1. Mở trình duyệt và truy cập hệ thống quản trị Airflow tại: `http://localhost:8999`
2. Đăng nhập với tài khoản được tự động tạo sẵn:
- Username: admin
- Password: admin
3. Tại giao diện chính (Tab DAGs), tìm dòng vietnamworks_daily_pipeline
4. Gạt nút công tắc ở cột ngoài cùng bên trái từ Pause (Trắng) sang Unpause (Xanh dương/ON).
5. Cách hoạt động: Vào đúng 22:00 hằng ngày, Airflow sẽ tự động bóp cò gọi file master_pipeline.py. File này sẽ cào 100 Job mới nhất, chạy qua bộ phận Data Quality Checks, từ chối nạp dữ liệu rác, và chỉ Vector hóa những Job chưa từng tồn tại trong hệ thống.


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

- [ ] Mở rộng Data Lake trên nền tảng Cloud (GCP/AWS) để xử lý lượng dữ liệu lớn hơn.
- [ ] Kết nối thêm các nguồn dữ liệu từ nền tảng khác để làm phong phú Data Warehouse.
- [ ] Phát triển tính năng Agentic AI tự động sinh Cover Letter dựa trên Job Description.
- [ ] Tích hợp thêm các model embedding, LLM cao cấp hơn và tối ưu thêm mô hình thuật toán vector search để tối ưu độ chính xác

---

## 👨‍💻 Tác giả (Author)

**[Thái Bảo An]** *Data Engineer*

*Nếu bạn thấy dự án này thú vị và hữu ích, đừng quên để lại 1 ⭐️ cho repository nhé!*
