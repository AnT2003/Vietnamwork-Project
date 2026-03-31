import os, sys, traceback
import warnings
import urllib3
import pandas as pd
from sqlalchemy import create_engine, text

# ==============================================================================
# 1. THIẾT LẬP MÔI TRƯỜNG & ĐƯỜNG DẪN HỆ THỐNG
# ==============================================================================
# Trỏ đường dẫn về thư mục gốc để import được các thư viện tự viết (scripts, config)
ROOT_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if ROOT_DIR not in sys.path:
    sys.path.append(ROOT_DIR)

from config import DB_URI, RAW_DIR, get_csv_files
# Đã xóa SQL_VALIDATE_NULL vì dùng DQ Rules mới
from scripts.sql_queries import SQL_TRUNCATE_STAGING, SQL_TRANSFORM_LOAD_DWH, SQL_VALIDATE_COUNT
from scripts.logger import log_start, log_success, log_fail
from scripts.crawl_vietnamworks import start_crawl 
from scripts.ai_tasks import run_generate_and_load_vectors

# Ép các thư viện AI và Request ngừng in ra các cảnh báo rác, giữ log Terminal sạch sẽ
warnings.filterwarnings('ignore')
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
os.environ['TOKENIZERS_PARALLELISM'] = 'false'
os.environ['TF_CPP_MIN_LOG_LEVEL'] = '3'

# ==============================================================================
# 2. CÁC HÀM XỬ LÝ DATABASE VÀ ETL CỐT LÕI
# ==============================================================================

# Hàm tạo cấu trúc nền móng: Chạy file SQL để xây các schema, tables và vector extension
def run_setup_database(conn):
    print("🛠️ Đang XÓA bảng cũ và CẬP NHẬT cấu trúc Database mới...")
    sql_path = os.path.join(ROOT_DIR, 'sql', 'setup_database.sql')
    with open(sql_path, 'r', encoding='utf-8') as f:
        conn.execute(text(f.read()))
    print("✅ Đã cập nhật cấu trúc Database thành công!")

# Hàm Load (L): Đổ toàn bộ dữ liệu CSV thô vào vùng đệm Staging bằng Bulk Insert
def run_load_to_staging(engine_obj, data_dir):
    # Dọn dẹp sạch vùng Staging trước khi nạp data mới để tránh rác
    with engine_obj.connect() as conn:
        with conn.begin():
            conn.execute(text(SQL_TRUNCATE_STAGING))
        
    csv_files = get_csv_files(data_dir)
    has_data = False
    
    # Sử dụng raw_connection và cursor để tăng tốc độ nạp thay vì dùng Pandas df.to_sql
    raw_conn = engine_obj.raw_connection()
    try:
        cursor = raw_conn.cursor()
        for table_name, file_path in csv_files.items():
            if os.path.exists(file_path):
                # Ép toàn bộ thành kiểu chuỗi (str) để chống lỗi sai định dạng ngày tháng/số
                df = pd.read_csv(file_path, dtype=str, keep_default_na=False)
                if not df.empty:
                    df = df.replace({'nan': '', 'None': '', 'null': ''})
                    
                    # Sinh lệnh SQL INSERT động tùy theo bảng
                    cols = ",".join([f'"{c}"' for c in df.columns])
                    placeholders = ",".join(["%s"] * len(df.columns))
                    table_id = f'staging."raw_{table_name}"'
                    sql = f"INSERT INTO {table_id} ({cols}) VALUES ({placeholders})"
                    
                    # Thực thi nạp hàng loạt (Bulk execute)
                    data_tuples = [tuple(x) for x in df.to_numpy()]
                    cursor.executemany(sql, data_tuples)
                    has_data = True
                    print(f"✅ Đã nạp thành công: raw_{table_name}")
        raw_conn.commit()
    except Exception as e:
        raw_conn.rollback() # Hoàn tác DB nếu có bất kỳ lỗi nào xảy ra
        raise e
    finally:
        cursor.close()
        raw_conn.close()
    return has_data

# Hàm Transform (T): Làm sạch, ép kiểu và đẩy dữ liệu từ Staging sang Data Warehouse
def run_transform_to_dwh(conn): 
    conn.execute(text(SQL_TRANSFORM_LOAD_DWH))

# ==============================================================================
# 3. KIỂM ĐỊNH CHẤT LƯỢNG DỮ LIỆU (DATA QUALITY - DQ)
# ==============================================================================
# 🟢 HÀM KIỂM ĐỊNH DATA QUALITY MỚI
def run_validation(engine):
    print("   [+] Đang thực thi Data Quality (DQ) Checks...")
    
    # Bộ quy tắc kiểm định 4 lớp: Toàn vẹn, Duy nhất, Nhất quán và Logic nghiệp vụ
    dq_rules = {
        # 1. Tính toàn vẹn (Completeness): Đảm bảo các khóa chính không bị NULL
        "PK_NULL_CHECK": {
            "query": "SELECT COUNT(*) FROM dwh.fact_job_postings WHERE job_id IS NULL OR company_id IS NULL",
            "error_msg": "Phát hiện Job ID hoặc Company ID bị NULL trong bảng Fact."
        },
        # 2. Tính duy nhất (Uniqueness): Chống Duplicate Job
        "DUPLICATE_JOB_CHECK": {
            "query": "SELECT (COUNT(job_id) - COUNT(DISTINCT job_id)) FROM dwh.dim_job_details",
            "error_msg": "Phát hiện Job ID bị trùng lặp (Duplicate) trong dim_job_details."
        },
        # 3. Tính nhất quán (Referential Integrity): Chống dữ liệu mồ côi (Orphan Check)
        "ORPHAN_COMPANY_CHECK": {
            "query": "SELECT COUNT(*) FROM dwh.fact_job_postings f LEFT JOIN dwh.dim_companies c ON f.company_id = c.company_id WHERE c.company_id IS NULL",
            "error_msg": "Có Job thuộc về một Công ty không tồn tại (Khóa ngoại mồ côi)."
        },
        # 4. Logic Nghiệp vụ (Business Logic): Kiểm tra các điều kiện thực tế
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
        # Chạy từng câu SQL kiểm định. Nếu có kết quả trả về > 0 nghĩa là có vi phạm
        for rule_id, rule_info in dq_rules.items():
            fail_count = conn.execute(text(rule_info["query"])).scalar()
            if fail_count > 0:
                error_logs.append(f"    ❌ [{rule_id}] {rule_info['error_msg']} ({fail_count} records vi phạm)")
    
    # Khóa sổ (Halt): Chặn đứng Pipeline nếu Data Quality bị Fail để bảo vệ hệ thống Dashboard/AI
    if error_logs:
        raise ValueError("DATA QUALITY FAILED:\n" + "\n".join(error_logs))
        
    print("   [+] ✅ Pass 100% Data Quality Checks!")
    # Lấy ra tổng số lượng bản ghi hợp lệ để báo cáo
    with engine.connect() as conn:
        return conn.execute(text(SQL_VALIDATE_COUNT)).scalar()

# ==============================================================================
# 4. CHƯƠNG TRÌNH CHÍNH (ORCHESTRATOR CHO INITIAL LOAD)
# Đây là script chạy 1 lần duy nhất khi khởi tạo hệ thống để cào khối lượng Data khổng lồ
# ==============================================================================
def run_initial_load():
    engine = create_engine(DB_URI)
    log_id = None
    print("🌟 BẮT ĐẦU INITIAL LOAD: CÀO DATA VÀ NẠP LẦN ĐẦU 🌟")
    
    # Chuẩn bị: Tạo lại cấu trúc Database và ghi log bắt đầu quá trình
    with engine.connect() as conn:
        with conn.begin(): 
            run_setup_database(conn)
            log_id = log_start(conn, 'vietnamworks_initial_load')

    try:
        # Giai đoạn 1: Mở Crawler chạy công suất cao (10.000 Jobs) để làm vốn dữ liệu ban đầu
        print("\n🚀 PHASE 1: CRAWLER (Đang cào dữ liệu gốc...)")
        start_crawl(target_total=10000, output_dir=RAW_DIR)

        # Giai đoạn 2: Xử lý ETL và duyệt chất lượng (DQ)
        print("\n🚀 PHASE 2: ETL (Đọc file gốc và đẩy vào DWH)")
        records_processed = 0
        if run_load_to_staging(engine, RAW_DIR):
            with engine.connect() as conn:
                with conn.begin(): 
                    run_transform_to_dwh(conn)
            # Gọi validation ở ngoài khối conn để nhận 'engine' chuẩn
            records_processed = run_validation(engine)

        # Giai đoạn 3: Gọi AI SentenceTransformer đọc tất cả Job để chuyển thành Vector Database
        print("\n🚀 PHASE 3: AI VECTORIZATION (Mã hóa toàn bộ data gốc)")
        run_generate_and_load_vectors(engine)

        # Giai đoạn 4: Hoàn thành, ghi log kết thúc thành công
        with engine.connect() as conn:
            with conn.begin(): 
                log_success(conn, log_id, records_processed)
            
        print(f"\n🎉 HOÀN TẤT NẠP LẦN ĐẦU! Database đã xử lý {records_processed} việc làm.")

    except Exception as e:
        # Nếu sập ở bất kỳ Phase nào, bắt lỗi và ghi fail vào database
        print(f"\n⚠️ LỖI INITIAL LOAD: {e}")
        if log_id:
            with engine.connect() as fail_conn:
                with fail_conn.begin(): 
                    log_fail(fail_conn, log_id, str(traceback.format_exc()))
        sys.exit(1)

if __name__ == "__main__":
    os.chdir(ROOT_DIR) 
    run_initial_load()