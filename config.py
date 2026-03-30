import os
from dotenv import load_dotenv

# 1. Định vị thư mục gốc của project
ROOT_DIR = os.path.dirname(os.path.abspath(__file__))

# 2. Nạp file .env vào hệ thống
# Lệnh này cực kỳ quan trọng nếu bạn chạy code trên máy thật (không dùng Docker)
load_dotenv(os.path.join(ROOT_DIR, '.env'))

# ==========================================
# 3. LẤY CÁC BIẾN MÔI TRƯỜNG (Có giá trị mặc định phòng hờ)
# ==========================================
# Dùng os.getenv("TÊN_BIẾN_TRONG_ENV", "Giá_trị_mặc_định_nếu_bị_lỗi_hoặc_quên_điền")
DB_URI = os.getenv("DB_URI", "postgresql://postgres:postgres@localhost:5455/vietnamworks")
EMBEDDING_MODEL_NAME = os.getenv("EMBEDDING_MODEL_NAME", 'sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2')

# ==========================================
# 4. CẤU HÌNH ĐƯỜNG DẪN THƯ MỤC DATA (GIỮ NGUYÊN)
# ==========================================
RAW_DIR = os.path.join(ROOT_DIR, 'data', 'raw')
DAILY_DIR = os.path.join(ROOT_DIR, 'data', 'daily')

os.makedirs(RAW_DIR, exist_ok=True)
os.makedirs(DAILY_DIR, exist_ok=True)

# Các cấu hình API Crawler giữ nguyên
API_SEARCH_URL = "https://ms.vietnamworks.com/job-search/v1.0/search"
HEADERS = {
    "Accept": "*/*",
    "Accept-Language": "vi",
    "Connection": "keep-alive",
    "Content-Type": "application/json",
    "Origin": "https://www.vietnamworks.com",
    "Referer": "https://www.vietnamworks.com/",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "X-Source": "Page-Container"
}

# Hàm tiện ích lấy file (giữ nguyên)
def get_csv_files(base_dir):
    return {
        "dim_categories": os.path.join(base_dir, "dim_categories.csv"),
        "dim_companies": os.path.join(base_dir, "dim_companies.csv"),
        "dim_industries": os.path.join(base_dir, "dim_industries.csv"),
        "dim_job_details": os.path.join(base_dir, "dim_job_details.csv"),
        "dim_locations": os.path.join(base_dir, "dim_locations.csv"),
        "dim_skills": os.path.join(base_dir, "dim_skills.csv"),
        "fact_job_postings": os.path.join(base_dir, "fact_job_postings.csv"),
    }