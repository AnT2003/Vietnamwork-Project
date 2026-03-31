import os, sys, traceback
import pandas as pd
from sqlalchemy import create_engine, text
import warnings
import urllib3

# ==========================================
# 1. ĐỊNH VỊ ĐƯỜNG DẪN HỆ THỐNG
# ==========================================
# Tự động trỏ về thư mục gốc của dự án để gọi được các module dùng chung (config, scripts)
ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT_DIR not in sys.path:
    sys.path.append(ROOT_DIR)

# ==========================================
# 2. IMPORT MODULES
# ==========================================
from config import DB_URI, DAILY_DIR, get_csv_files
from scripts.sql_queries import SQL_TRUNCATE_STAGING, SQL_TRANSFORM_LOAD_DWH, SQL_VALIDATE_COUNT
from scripts.logger import log_start, log_success, log_fail
from scripts.crawl_vietnamworks import start_crawl 
from scripts.ai_tasks import run_generate_and_load_vectors

# ==========================================
# 3. CẤU HÌNH TẮT CẢNH BÁO
# ==========================================
# Vô hiệu hóa các cảnh báo không cần thiết từ thư viện AI và Request để Terminal gọn gàng
warnings.filterwarnings('ignore')
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
os.environ['TOKENIZERS_PARALLELISM'] = 'false'
os.environ['TF_CPP_MIN_LOG_LEVEL'] = '3'

# ==========================================
# 4. HÀM TỰ ĐỘNG KHỞI TẠO DATABASE (AN TOÀN)
# ==========================================
def check_and_setup_database(engine):
    # Kiểm tra xem bảng fact trung tâm đã tồn tại trong Data Warehouse chưa
    check_sql = "SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_schema = 'dwh' AND table_name = 'fact_job_postings');"
    with engine.connect() as conn:
        try:
            table_exists = conn.execute(text(check_sql)).scalar()
        except Exception:
            table_exists = False 

    # Nếu chưa có (chạy lần đầu), tự động đọc file setup_database.sql để xây dựng cấu trúc DB
    if not table_exists:
        print("⚠️ Phát hiện Database trống! Đang tự động khởi tạo cấu trúc lần đầu...")
        sql_path = os.path.join(ROOT_DIR, 'sql', 'setup_database.sql')
        if os.path.exists(sql_path):
            with open(sql_path, 'r', encoding='utf-8') as f:
                with engine.begin() as conn:
                    conn.execute(text(f.read()))
            print("✅ Đã khởi tạo cấu trúc Database thành công!")
        else:
            raise FileNotFoundError(f"Không tìm thấy file SQL tại: {sql_path}")
    else:
        print("✅ Cấu trúc Database đã sẵn sàng.")

# ==========================================
# 5. CÁC HÀM XỬ LÝ ETL (Extract - Transform - Load)
# ==========================================

# Khâu E & L: Đọc file CSV và nạp thô vào vùng Staging (Vùng đệm)
def run_load_to_staging(engine, data_dir):
    # Truncate & Load: Xóa sạch dữ liệu cũ trong Staging trước khi nạp data mới ngày hôm nay
    with engine.begin() as conn:
        conn.execute(text(SQL_TRUNCATE_STAGING))
        
    csv_files = get_csv_files(data_dir)
    has_data = False
    
    # Sử dụng raw_connection và cursor để tăng tốc độ nạp dữ liệu (Bulk Insert) thay vì pd.to_sql
    raw_conn = engine.raw_connection()
    try:
        cursor = raw_conn.cursor()
        for table_name, file_path in csv_files.items():
            if os.path.exists(file_path):
                # Đọc CSV, ép tất cả thành chuỗi (str) để tránh lỗi định dạng và xử lý dữ liệu rỗng (NaN)
                df = pd.read_csv(file_path, dtype=str, keep_default_na=False)
                if not df.empty:
                    df = df.replace({'nan': '', 'None': '', 'null': ''})
                    
                    # Cấu trúc câu lệnh INSERT động theo số lượng cột của DataFrame
                    cols = ",".join([f'"{c}"' for c in df.columns])
                    placeholders = ",".join(["%s"] * len(df.columns))
                    table_id = f'staging."raw_{table_name}"'
                    sql = f"INSERT INTO {table_id} ({cols}) VALUES ({placeholders})"
                    
                    # Chuyển Dataframe thành Tuple array và thực thi Bulk Insert
                    data_tuples = [tuple(x) for x in df.to_numpy()]
                    cursor.executemany(sql, data_tuples)
                    has_data = True
                    print(f"✅ Đã nạp thành công: raw_{table_name}")
        raw_conn.commit()
    except Exception as e:
        raw_conn.rollback() # Hoàn tác nếu có lỗi để bảo vệ DB
        raise e
    finally:
        cursor.close()
        raw_conn.close()
        
    return has_data

# Khâu T: Transform dữ liệu từ Staging (thô) sang Data Warehouse (chuẩn hóa Snowflake)
def run_transform_to_dwh(engine): 
    with engine.begin() as conn:
        conn.execute(text(SQL_TRANSFORM_LOAD_DWH))

# HỆ THỐNG DATA QUALITY CHECK HOÀN CHỈNH
def run_validation(engine):
    print("   [+] Đang thực thi Data Quality (DQ) Checks...")
    
    # Định nghĩa các bộ quy tắc kiểm tra tiêu chuẩn quốc tế
    dq_rules = {
        # 1. Tính toàn vẹn (Completeness): Đảm bảo Khóa chính (PK) không bao giờ trống
        "PK_NULL_CHECK": {
            "query": "SELECT COUNT(*) FROM dwh.fact_job_postings WHERE job_id IS NULL OR company_id IS NULL",
            "error_msg": "Phát hiện Job ID hoặc Company ID bị NULL trong bảng Fact."
        },
        # 2. Tính duy nhất (Uniqueness): Đảm bảo không có ID bị trùng lặp sinh ra lỗi thống kê
        "DUPLICATE_JOB_CHECK": {
            "query": "SELECT (COUNT(job_id) - COUNT(DISTINCT job_id)) FROM dwh.dim_job_details",
            "error_msg": "Phát hiện Job ID bị trùng lặp (Duplicate) trong dim_job_details."
        },
        # 3. Tính nhất quán (Referential Integrity): Kiểm tra khóa ngoại bị đứt gãy (Orphan Data)
        "ORPHAN_COMPANY_CHECK": {
            "query": "SELECT COUNT(*) FROM dwh.fact_job_postings f LEFT JOIN dwh.dim_companies c ON f.company_id = c.company_id WHERE c.company_id IS NULL",
            "error_msg": "Có Job thuộc về một Công ty không tồn tại (Khóa ngoại mồ côi)."
        },
        # 4. Logic Nghiệp vụ (Business Logic): Kiểm tra các điều kiện phi logic của dữ liệu thực tế
        "NEGATIVE_SALARY_CHECK": {
            "query": "SELECT COUNT(*) FROM dwh.dim_job_details WHERE salary_numeric < 0",
            "error_msg": "Phát hiện công việc có mức lương bị ÂM."
        },
        "LOGICAL_DATE_CHECK": {
            "query": "SELECT COUNT(*) FROM dwh.dim_job_details WHERE posted_date > expiry_date",
            "error_msg": "Ngày đăng tuyển (posted_date) lại lớn hơn ngày hết hạn (expiry_date)."
        }
    }
    
    error_logs = []
    with engine.connect() as conn:
        # Vòng lặp quét qua từng luật để chạy câu SQL kiểm duyệt
        for rule_id, rule_info in dq_rules.items():
            fail_count = conn.execute(text(rule_info["query"])).scalar()
            if fail_count > 0:
                # Nếu fail_count > 0 tức là có bản ghi vi phạm luật
                error_logs.append(f"    ❌ [{rule_id}] {rule_info['error_msg']} ({fail_count} records vi phạm)")
    
    # Kích hoạt ngắt hệ thống (Pipeline thất bại) nếu phát hiện bất kỳ vi phạm nào để cấm data rác vào Dashboard
    if error_logs:
        raise ValueError("DATA QUALITY FAILED:\n" + "\n".join(error_logs))
        
    print("   [+] ✅ Pass 100% Data Quality Checks!")
    # Trả về tổng số lượng bản ghi đã được nạp thành công để ghi log
    with engine.connect() as conn:
        return conn.execute(text(SQL_VALIDATE_COUNT)).scalar()

# Hàm dọn dẹp các file rác trung gian sau khi đã nạp xong để tiết kiệm dung lượng ổ cứng
def clean_up_daily_data(data_dir):
    for file_path in get_csv_files(data_dir).values():
        if os.path.exists(file_path): 
            os.remove(file_path)

# ==========================================
# 6. LUỒNG CHẠY CHÍNH (ORCHESTRATOR / PIPELINE DAG)
# ==========================================
def run_daily_pipeline():
    engine = create_engine(DB_URI)
    log_id = None
    print("🌟 BẮT ĐẦU MASTER PIPELINE (DELTA LOAD HẰNG NGÀY) 🌟")
    
    try:
        # 0. Chuẩn bị nền móng & Ghi log bắt đầu
        check_and_setup_database(engine)
        with engine.begin() as conn:
            log_id = log_start(conn, 'vietnamworks_daily_sync')

        # 1. Cào dữ liệu thô từ Website (Chỉ lấy job mới)
        print("\n🚀 PHASE 1: CRAWLER (Lấy 100 Jobs mới...)")
        start_crawl(target_total=100, output_dir=DAILY_DIR)
        
        # 2. Extract - Transform - Load và kiểm tra chất lượng (DQ)
        print("\n🚀 PHASE 2: ETL (Chỉ đẩy Data mới cào vào DWH)")
        records_processed = 0
        if run_load_to_staging(engine, DAILY_DIR):
            run_transform_to_dwh(engine)
            records_processed = run_validation(engine)
            clean_up_daily_data(DAILY_DIR)

        # 3. Tạo Embedding bằng AI Model cho hệ thống RAG Chatbot
        print("\n🚀 PHASE 3: AI VECTORIZATION (Chỉ Vector hóa Job chưa có)")
        run_generate_and_load_vectors(engine)

        # 4. Đánh dấu hoàn tất và kết thúc log
        with engine.begin() as conn:
            log_success(conn, log_id, records_processed)
            
        print("\n🎉 HOÀN TẤT PIPELINE HẰNG NGÀY!")

    except Exception as e:
        # Xử lý ngoại lệ: In lỗi và cập nhật trạng thái thất bại vào bảng Log
        print(f"\n⚠️ LỖI PIPELINE HẰNG NGÀY: {e}")
        if log_id:
            try:
                with engine.begin() as fail_conn:
                    log_fail(fail_conn, log_id, str(traceback.format_exc()))
            except: pass
        sys.exit(1) # Báo lỗi ra hệ điều hành / Airflow để biết luồng đã sập

if __name__ == "__main__":
    os.chdir(ROOT_DIR)
    run_daily_pipeline()