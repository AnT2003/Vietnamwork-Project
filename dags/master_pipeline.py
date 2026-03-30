import os, sys, traceback
import pandas as pd
from sqlalchemy import create_engine, text
import warnings
import urllib3

# ==========================================
# 1. ĐỊNH VỊ ĐƯỜNG DẪN HỆ THỐNG
# ==========================================
ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT_DIR not in sys.path:
    sys.path.append(ROOT_DIR)

# ==========================================
# 2. IMPORT MODULES
# ==========================================
from config import DB_URI, DAILY_DIR, get_csv_files
from scripts.sql_queries import SQL_TRUNCATE_STAGING, SQL_TRANSFORM_LOAD_DWH, SQL_VALIDATE_NULL, SQL_VALIDATE_COUNT
from scripts.logger import log_start, log_success, log_fail
from scripts.crawl_vietnamworks import start_crawl 
from scripts.ai_tasks import run_generate_and_load_vectors

# ==========================================
# 3. CẤU HÌNH TẮT CẢNH BÁO
# ==========================================
warnings.filterwarnings('ignore')
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
os.environ['TOKENIZERS_PARALLELISM'] = 'false'
os.environ['TF_CPP_MIN_LOG_LEVEL'] = '3'

# ==========================================
# 4. HÀM TỰ ĐỘNG KHỞI TẠO DATABASE (AN TOÀN)
# ==========================================
def check_and_setup_database(conn):
    # Dùng SQL ngó thử xem bảng chính đã tồn tại trong DWH chưa
    check_sql = "SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_schema = 'dwh' AND table_name = 'fact_job_postings');"
    try:
        table_exists = conn.execute(text(check_sql)).scalar()
    except Exception:
        # Nếu lỗi (có thể do chưa có schema dwh), coi như là bảng chưa tồn tại
        table_exists = False 

    if not table_exists:
        print("⚠️ Phát hiện Database trống! Đang tự động khởi tạo cấu trúc lần đầu...")
        sql_path = os.path.join(ROOT_DIR, 'sql', 'setup_database.sql')
        if os.path.exists(sql_path):
            with open(sql_path, 'r', encoding='utf-8') as f:
                conn.execute(text(f.read()))
            print("✅ Đã khởi tạo cấu trúc Database thành công!")
        else:
            raise FileNotFoundError(f"Không tìm thấy file SQL tại: {sql_path}")
    else:
        print("✅ Cấu trúc Database đã sẵn sàng, bỏ qua bước khởi tạo.")

# ==========================================
# 5. CÁC HÀM XỬ LÝ ETL
# ==========================================
def run_load_to_staging(conn, data_dir):
    conn.execute(text(SQL_TRUNCATE_STAGING))
    csv_files = get_csv_files(data_dir)
    has_data = False
    
    for table_name, file_path in csv_files.items():
        if os.path.exists(file_path):
            df = pd.read_csv(file_path, dtype=str, keep_default_na=False)
            if not df.empty:
                df = df.replace({'nan': '', 'None': '', 'null': ''})
                df.to_sql(f"raw_{table_name}", con=conn, schema='staging', if_exists='append', index=False, method='multi', chunksize=1000)
                has_data = True
    return has_data

def run_transform_to_dwh(conn): 
    conn.execute(text(SQL_TRANSFORM_LOAD_DWH))

def run_validation(conn):
    if conn.execute(text(SQL_VALIDATE_NULL)).scalar() > 0: 
        raise ValueError("PK bị NULL.")
    return conn.execute(text(SQL_VALIDATE_COUNT)).scalar()

def clean_up_daily_data(data_dir):
    for file_path in get_csv_files(data_dir).values():
        if os.path.exists(file_path): 
            os.remove(file_path)

# ==========================================
# 6. LUỒNG CHẠY CHÍNH
# ==========================================
def run_daily_pipeline():
    engine = create_engine(DB_URI)
    log_id = None
    print("🌟 BẮT ĐẦU MASTER PIPELINE (DELTA LOAD HẰNG NGÀY) 🌟")
    
    try:
        # KIỂM TRA & LOG KHỞI ĐỘNG
        with engine.connect() as conn:
            with conn.begin(): 
                check_and_setup_database(conn)
                log_id = log_start(conn, 'vietnamworks_daily_sync')

        print("\n🚀 PHASE 1: CRAWLER (Lấy 100 Jobs mới...)")
        start_crawl(target_total=100, output_dir=DAILY_DIR)
        
        print("\n🚀 PHASE 2: ETL (Chỉ đẩy Data mới cào vào DWH)")
        records_processed = 0
        with engine.connect() as conn:
            with conn.begin(): 
                if run_load_to_staging(conn, DAILY_DIR):
                    run_transform_to_dwh(conn)
                    records_processed = run_validation(conn)
                    clean_up_daily_data(DAILY_DIR)

        print("\n🚀 PHASE 3: AI VECTORIZATION (Chỉ Vector hóa Job chưa có)")
        with engine.connect() as conn:
            with conn.begin(): 
                run_generate_and_load_vectors(conn)

        with engine.connect() as conn:
            with conn.begin(): 
                log_success(conn, log_id, records_processed)
            
        print("\n🎉 HOÀN TẤT PIPELINE HẰNG NGÀY!")

    except Exception as e:
        print(f"\n⚠️ LỖI PIPELINE HẰNG NGÀY: {e}")
        if log_id:
            with engine.connect() as fail_conn:
                with fail_conn.begin(): 
                    log_fail(fail_conn, log_id, str(traceback.format_exc()))
        sys.exit(1)

if __name__ == "__main__":
    os.chdir(ROOT_DIR)
    run_daily_pipeline()