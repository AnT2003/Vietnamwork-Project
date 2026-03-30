import os, sys
import pandas as pd
import re
import warnings
from sentence_transformers import SentenceTransformer

# 1. Định vị ra ngoài thư mục gốc (Root)
ROOT_DIR = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
if ROOT_DIR not in sys.path:
    sys.path.append(ROOT_DIR)

# Chặn cảnh báo
warnings.filterwarnings("ignore", category=DeprecationWarning)
warnings.filterwarnings("ignore", message=".*align should be passed.*")

try:
    from pyvi import ViTokenizer
except ImportError:
    ViTokenizer = None

from config import EMBEDDING_MODEL_NAME
from scripts.sql_queries import SQL_FETCH_JOBS_FOR_EMBEDDING

VIETNAMESE_STOPWORDS = {"và", "hoặc", "của", "các", "có", "được", "cho", "trong", "một", "là", "với", "những", "thì", "để", "sẽ", "đã", "ở", "như", "nào", "về"}

def nlp_transform_text(text_data):
    if pd.isna(text_data) or not isinstance(text_data, str): 
        return ""
    text_data = text_data.lower()
    text_data = re.sub(r'[^\w\s]', ' ', text_data)
    text_data = re.sub(r'\s+', ' ', text_data).strip()
    
    if ViTokenizer:
        text_data = ViTokenizer.tokenize(text_data)
        
    words = text_data.split()
    filtered_words = [w for w in words if w.replace('_', ' ') not in VIETNAMESE_STOPWORDS]
    
    return " ".join(filtered_words)

def run_generate_and_load_vectors(engine):
    print("⏳ Đang tải mô hình AI (SentenceTransformer)...")
    model = SentenceTransformer(EMBEDDING_MODEL_NAME)
    
    # 🟢 SỬA LỖI TẬN GỐC: Dùng pure cursor để đọc data, không dùng pd.read_sql
    print("📥 Lấy toàn bộ dữ liệu công việc từ DWH...")
    raw_conn = engine.raw_connection()
    try:
        cursor = raw_conn.cursor()
        
        # Lấy danh sách việc làm cần xử lý
        cursor.execute(SQL_FETCH_JOBS_FOR_EMBEDDING)
        cols = [desc[0] for desc in cursor.description]
        jobs_data = cursor.fetchall()
        df = pd.DataFrame(jobs_data, columns=cols)
        
        if df.empty: 
            print("✅ DWH đang trống, không có gì để Vector hóa.")
            return 0
            
        # Lấy danh sách Vector đã tồn tại
        print("🔍 Đang đối chiếu để tìm các công việc mới...")
        cursor.execute("SELECT job_id FROM vector_dwh.dim_job_vectors")
        existing_job_ids = {str(row[0]) for row in cursor.fetchall()}
        
    except Exception as e:
        print(f"⚠️ Lỗi khi đọc dữ liệu Database: {e}")
        raise e
    finally:
        cursor.close()
        raw_conn.close()
    
    # Lọc dữ liệu mới
    df['job_id'] = df['job_id'].astype(str)
    df_new = df[~df['job_id'].isin(existing_job_ids)].copy()
    
    if df_new.empty:
        print("✅ Tất cả công việc trong DWH đều đã có Vector. Bỏ qua chạy AI!")
        return 0
        
    print(f"🚀 Bắt đầu NLP Transform và tạo Vector cho {len(df_new)} công việc MỚI...")

    clean_chunks = []
    for _, row in df_new.iterrows():
        raw_chunk = (
            f"vị trí {row.get('job_title', '')} cấp bậc {row.get('job_level', '')} công ty {row.get('company_name', '')} "
            f"địa điểm {row.get('locations', '')} ngành nghề {row.get('industry_name', '')} "
            f"lương {row.get('salary_text', '')} kinh nghiệm {row.get('years_of_experience', '')} "
            f"kỹ năng {row.get('skills', '')} phúc lợi {row.get('job_benefits', '')} "
            f"mô tả {row.get('job_description', '')} yêu cầu {row.get('job_requirements', '')}"
        )
        clean_chunks.append(nlp_transform_text(raw_chunk))
    
    print(f"🧠 AI đang nhúng {len(clean_chunks)} khối văn bản...")
    embeddings = model.encode(clean_chunks, batch_size=32, show_progress_bar=True)
    embedding_strs = ["[" + ",".join(map(str, emb)) + "]" for emb in embeddings]

    # Nạp dữ liệu Vector vào DB bằng Cursor
    print("💾 Đang nạp Vector mới vào Database...")
    raw_conn_write = engine.raw_connection()
    try:
        cursor_write = raw_conn_write.cursor()
        
        upsert_sql = """
            INSERT INTO vector_dwh.dim_job_vectors (job_id, chunk_text, embedding)
            VALUES (%s, %s, %s)
            ON CONFLICT (job_id) DO UPDATE SET
                chunk_text = EXCLUDED.chunk_text,
                embedding = EXCLUDED.embedding
        """
        
        insert_data = [
            (row.job_id, clean_chunks[idx], embedding_strs[idx])
            for idx, row in enumerate(df_new.itertuples())
        ]
        
        cursor_write.executemany(upsert_sql, insert_data)
        raw_conn_write.commit()
        
        print(f"🎉 Đã nạp thành công {len(insert_data)} Vector mới!")
        return len(insert_data)
        
    except Exception as e:
        raw_conn_write.rollback()
        print(f"⚠️ Lỗi khi nạp Vector: {e}")
        raise e
    finally:
        cursor_write.close()
        raw_conn_write.close()