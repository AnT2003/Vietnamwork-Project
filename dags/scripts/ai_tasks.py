import os, sys
import pandas as pd
import re
import warnings
from sentence_transformers import SentenceTransformer

# 1. Định vị ra ngoài thư mục gốc (Root) để import được các module cấu hình
ROOT_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if ROOT_DIR not in sys.path:
    sys.path.append(ROOT_DIR)

# Chặn các cảnh báo rác từ thư viện AI để Terminal gọn gàng hơn
warnings.filterwarnings("ignore", category=DeprecationWarning)
warnings.filterwarnings("ignore", message=".*align should be passed.*")

# Thử import Pyvi để phân từ tiếng Việt nếu không có thì bỏ qua
try:
    from pyvi import ViTokenizer
except ImportError:
    ViTokenizer = None

from config import EMBEDDING_MODEL_NAME
from scripts.sql_queries import SQL_FETCH_JOBS_FOR_EMBEDDING

# Tập hợp các từ dừng (stopwords) tiếng Việt để loại bỏ các từ không mang ý nghĩa tìm kiếm
VIETNAMESE_STOPWORDS = {"và", "hoặc", "của", "các", "có", "được", "cho", "trong", "một", "là", "với", "những", "thì", "để", "sẽ", "đã", "ở", "như", "nào", "về"}

# ==============================================================================
# Hàm tiền xử lý văn bản (NLP): Làm sạch, chuẩn hóa và phân từ tiếng Việt
# ==============================================================================
def nlp_transform_text(text_data):
    if pd.isna(text_data) or not isinstance(text_data, str): 
        return ""
    
    # Chuyển chữ thường và thay thế các ký tự đặc biệt (dấu câu) bằng khoảng trắng
    text_data = text_data.lower()
    text_data = re.sub(r'[^\w\s]', ' ', text_data)
    text_data = re.sub(r'\s+', ' ', text_data).strip()
    
    # Phân từ tiếng Việt (Ví dụ: "kỹ năng" -> "kỹ_năng") giúp AI hiểu đúng cụm từ
    if ViTokenizer:
        text_data = ViTokenizer.tokenize(text_data)
        
    # Loại bỏ các từ stopwords
    words = text_data.split()
    filtered_words = [w for w in words if w.replace('_', ' ') not in VIETNAMESE_STOPWORDS]
    
    return " ".join(filtered_words)


# ==============================================================================
# Hàm cốt lõi: Đọc dữ liệu mới, nhúng AI (Embedding) và lưu Vector vào Database
# ==============================================================================
def run_generate_and_load_vectors(engine):
    print("⏳ Đang tải mô hình AI (SentenceTransformer)...")
    model = SentenceTransformer(EMBEDDING_MODEL_NAME)
    
    # không dùng pd.read_sql để tránh lỗi quá tải bộ nhớ (OOM) hoặc lỗi driver
    print("📥 Lấy toàn bộ dữ liệu công việc từ DWH...")
    raw_conn = engine.raw_connection()
    try:
        cursor = raw_conn.cursor()
        
        # Lấy toàn bộ danh sách chi tiết việc làm từ Data Warehouse
        cursor.execute(SQL_FETCH_JOBS_FOR_EMBEDDING)
        cols = [desc[0] for desc in cursor.description]
        jobs_data = cursor.fetchall()
        df = pd.DataFrame(jobs_data, columns=cols)
        
        if df.empty: 
            print("✅ DWH đang trống, không có gì để Vector hóa.")
            return 0
            
        # Lấy danh sách ID các Job ĐÃ CÓ Vector trong Database để làm Delta Load (Chỉ xử lý Job mới)
        print("🔍 Đang đối chiếu để tìm các công việc mới...")
        cursor.execute("SELECT job_id FROM vector_dwh.dim_job_vectors")
        existing_job_ids = {str(row[0]) for row in cursor.fetchall()}
        
    except Exception as e:
        print(f"⚠️ Lỗi khi đọc dữ liệu Database: {e}")
        raise e
    finally:
        cursor.close()
        raw_conn.close()
    
    # Lọc ra các Job chưa từng được Vector hóa (Job Mới)
    df['job_id'] = df['job_id'].astype(str)
    df_new = df[~df['job_id'].isin(existing_job_ids)].copy()
    
    if df_new.empty:
        print("✅ Tất cả công việc trong DWH đều đã có Vector. Bỏ qua chạy AI!")
        return 0
        
    print(f"🚀 Bắt đầu NLP Transform và tạo Vector cho {len(df_new)} công việc MỚI...")

    # Gom toàn bộ thông tin quan trọng của một Job thành một đoạn văn bản duy nhất 
    clean_chunks = []
    for _, row in df_new.iterrows():
        raw_chunk = (
            f"vị trí {row.get('job_title', '')} cấp bậc {row.get('job_level', '')} công ty {row.get('company_name', '')} "
            f"địa điểm {row.get('locations', '')} ngành nghề {row.get('industry_name', '')} "
            f"lương {row.get('salary_text', '')} kinh nghiệm {row.get('years_of_experience', '')} "
            f"kỹ năng {row.get('skills', '')} phúc lợi {row.get('job_benefits', '')} "
            f"mô tả {row.get('job_description', '')} yêu cầu {row.get('job_requirements', '')}"
        )
        # Làm sạch đoạn văn bản vừa gom
        clean_chunks.append(nlp_transform_text(raw_chunk))
    # Mã hóa các đoạn văn bản thành Vector 384 chiều (Chạy theo batch_size để tối ưu RAM)
    print(f"🧠 AI đang nhúng {len(clean_chunks)} khối văn bản...")
    embeddings = model.encode(clean_chunks, batch_size=32, show_progress_bar=True)
    # Chuyển đổi mảng Vector thành chuỗi định dạng "[v1, v2, ...]" để pgvector có thể hiểu được
    embedding_strs = ["[" + ",".join(map(str, emb)) + "]" for emb in embeddings]
    # Mở kết nối ghi để nạp dữ liệu Vector vào Database
    print("💾 Đang nạp Vector mới vào Database...")
    raw_conn_write = engine.raw_connection()
    try:
        cursor_write = raw_conn_write.cursor()
        # Câu lệnh Upsert: Nếu Job ID chưa có thì INSERT, nếu có rồi thì UPDATE (Chống trùng lặp tuyệt đối)
        upsert_sql = """
            INSERT INTO vector_dwh.dim_job_vectors (job_id, chunk_text, embedding)
            VALUES (%s, %s, %s)
            ON CONFLICT (job_id) DO UPDATE SET
                chunk_text = EXCLUDED.chunk_text,
                embedding = EXCLUDED.embedding
        """
        # Đóng gói dữ liệu để ghi hàng loạt
        insert_data = [
            (row.job_id, clean_chunks[idx], embedding_strs[idx])
            for idx, row in enumerate(df_new.itertuples())
        ]
        cursor_write.executemany(upsert_sql, insert_data)
        raw_conn_write.commit()
        print(f"🎉 Đã nạp thành công {len(insert_data)} Vector mới!")
        return len(insert_data)
    except Exception as e:
        # Nếu có lỗi giữa chừng, Rollback lại toàn bộ giao dịch để bảo toàn data
        raw_conn_write.rollback()
        print(f"⚠️ Lỗi khi nạp Vector: {e}")
        raise e
    finally:
        cursor_write.close()
        raw_conn_write.close()