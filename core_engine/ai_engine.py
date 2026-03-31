import os
import json
import re
import pandas as pd
import streamlit as st
from sqlalchemy import create_engine, text
from sentence_transformers import SentenceTransformer
from pyvi import ViTokenizer
import PyPDF2
from ollama import Client
from config import DB_URI

EMBEDDING_MODEL_NAME = 'sentence-transformers/paraphrase-multilingual-MiniLM-L12-v2'
CLOUD_LLM_MODEL = 'gpt-oss:120b-cloud'

@st.cache_resource
def load_embedding_model():
    return SentenceTransformer(EMBEDDING_MODEL_NAME)

embed_model = load_embedding_model()
engine = create_engine(DB_URI)

def nlp_transform_text(text_input):
    if not text_input: return ""
    return ViTokenizer.tokenize(str(text_input).lower())

def extract_text_from_pdf(pdf_file):
    try:
        reader = PyPDF2.PdfReader(pdf_file)
        return "".join([page.extract_text() + " " for page in reader.pages]).strip()
    except Exception: return ""

def extract_search_intent(query, cv_text):
    prompt = f"""Phân tích yêu cầu tìm việc của người dùng: "{query}"\n[THÔNG TIN CV]:\n{cv_text[:1500] if cv_text else "Không có."}

Nhiệm vụ của bạn là trả về một chuỗi JSON duy nhất (KHÔNG CÓ TEXT NÀO KHÁC):
1. "job_title": Trích xuất chức danh công việc. NẾU câu hỏi chung chung (ví dụ "tìm việc theo CV"), BẮT BUỘC tự đọc phần [CV] và trích xuất chức danh chuyên môn chính. Nếu không có, để rỗng "".
2. "locations": Mảng các địa phương tìm việc kèm từ đồng nghĩa (VD: ["hà nội", "ha noi", "hn"]). Nếu không đề cập, để mảng rỗng [].
Định dạng: {{"job_title": "...", "locations": ["...", "..."]}}
"""
    client = Client(host="https://ollama.com", headers={'Authorization': 'Bearer ' + os.environ.get('OLLAMA_API_KEY', '')})
    try:
        res = client.chat(model=CLOUD_LLM_MODEL, messages=[{'role': 'user', 'content': prompt}], format='json')
        return json.loads(res['message']['content'])
    except Exception: return {"job_title": "", "locations": []}

def fetch_and_rank_jobs(user_query, cv_text, top_k=10):
    user_query_lower = user_query.lower()
    
    # 1. LLM Trích xuất Ý định nguyên bản
    intent = extract_search_intent(user_query, cv_text)
    job_kw = intent.get('job_title', '').strip().lower()
    loc_list = intent.get('locations', [])
    target_synonyms = [str(l).strip().lower() for l in loc_list if l]
            
    # 2. Xây dựng Vector Search
    search_query = f"{job_kw} {user_query}"
    if cv_text: search_query += f" {' '.join(cv_text[:500].split())}"
    query_vector = embed_model.encode(nlp_transform_text(search_query)).tolist()
    vector_str = "[" + ",".join(map(str, query_vector)) + "]"
    
    # Hàm sinh SQL động (Hybrid Retrieval: Vector + Keyword)
    def get_jobs_from_db(use_location=True):
        where_clauses = ["1=1"]
        params = {"query_vector": vector_str}
        
        # SQL LỌC ĐỊA ĐIỂM: Chống lỗi "hn" bắt nhầm "tnhh" bằng Regex \b
        if use_location and target_synonyms:
            loc_conditions = []
            for i, syn in enumerate(target_synonyms):
                if len(syn) <= 3:
                    loc_conditions.append(f"(v.chunk_text ~* '\\b{syn}\\b' OR c.company_name ~* '\\b{syn}\\b' OR d.job_title ~* '\\b{syn}\\b')")
                else:
                    loc_conditions.append(f"(LOWER(v.chunk_text) LIKE :loc_{i} OR LOWER(c.company_name) LIKE :loc_{i} OR LOWER(d.job_title) LIKE :loc_{i})")
                    params[f'loc_{i}'] = f"%{syn}%"
            where_clauses.append(f"({' OR '.join(loc_conditions)})")

        where_str = " AND ".join(where_clauses)
        
        # SQL LỌC TỪ KHÓA BẮT BUỘC: Ép Database tìm chính xác các từ cấu thành chức danh
        kw_sql = ""
        if job_kw:
            job_words = [w for w in job_kw.split() if len(w) > 0]
            kw_conds = []
            for i, w in enumerate(job_words):
                if len(w) <= 3:
                    kw_conds.append(f"d.job_title ~* '\\b{w}\\b'")
                else:
                    kw_conds.append(f"LOWER(d.job_title) LIKE :kw_{i}")
                    params[f"kw_{i}"] = f"%{w}%"
            
            kw_str = " AND ".join(kw_conds) if kw_conds else "1=1"
            
            # Tạo truy vấn song song (UNION) để vét cạn Job bị miss
            kw_sql = f"""
            , KeywordSearch AS (
                SELECT CAST(d.job_id AS VARCHAR) AS job_id, d.job_title, c.company_name, d.salary_text, d.job_url, v.chunk_text,
                       0.0 AS vector_distance
                FROM vector_dwh.dim_job_vectors v 
                JOIN dwh.dim_job_details d ON CAST(v.job_id AS VARCHAR) = CAST(d.job_id AS VARCHAR)
                JOIN dwh.fact_job_postings f ON d.job_id = f.job_id JOIN dwh.dim_companies c ON f.company_id = c.company_id
                WHERE {where_str} AND ({kw_str}) LIMIT 300
            )
            """
        
        sql = f"""
            WITH VectorSearch AS (
                SELECT CAST(d.job_id AS VARCHAR) AS job_id, d.job_title, c.company_name, d.salary_text, d.job_url, v.chunk_text,
                       (v.embedding <=> :query_vector) AS vector_distance
                FROM vector_dwh.dim_job_vectors v 
                JOIN dwh.dim_job_details d ON CAST(v.job_id AS VARCHAR) = CAST(d.job_id AS VARCHAR)
                JOIN dwh.fact_job_postings f ON d.job_id = f.job_id JOIN dwh.dim_companies c ON f.company_id = c.company_id
                WHERE {where_str} ORDER BY vector_distance ASC LIMIT 300
            )
            {kw_sql}
            SELECT * FROM VectorSearch { 'UNION SELECT * FROM KeywordSearch' if kw_sql else '' };
        """
        with engine.connect() as conn:
            res = conn.execute(text(sql), params)
            return pd.DataFrame(res.fetchall(), columns=res.keys()).drop_duplicates(subset=['job_id'])

    # Chạy lần 1: Lọc tại địa phương
    df_jobs = get_jobs_from_db(use_location=True)
    fallback_triggered = False
    requested_loc = ", ".join(target_synonyms[:2]) if target_synonyms else ""
    
    # Chạy lần 2: Tự động Fallback toàn quốc nếu không có Job
    if df_jobs.empty and target_synonyms:
        fallback_triggered = True
        df_jobs = get_jobs_from_db(use_location=False)
        
    if df_jobs.empty: return pd.DataFrame(), fallback_triggered, requested_loc

    # 3. CHẤM ĐIỂM LEXICAL (DIỆT TẬN GỐC RÁC HIỂN THỊ)
    stopwords = ['tìm', 'việc', 'làm', 'ở', 'tại', 'cho', 'có', 'cần', 'những', 'một', 'vị trí', 'nhân viên']
    clean_query = user_query_lower
    for syn in target_synonyms: clean_query = clean_query.replace(syn, ' ')
    query_keywords = [w.strip(',.?!"()') for w in clean_query.split() if w.strip(',.?!"()') not in stopwords and len(w) > 1]
    job_words = [w for w in job_kw.split() if len(w) > 0]

    def compute_lexical_score(row):
        title = str(row['job_title']).lower()
        chunk = str(row['chunk_text']).lower()
        score = 0
        
        # Tuyệt đối ưu tiên job chứa nguyên cụm chức danh
        if job_kw and job_kw in title: score += 10000
        elif job_kw and job_kw in chunk: score += 1000
        
        # Ưu tiên các job chứa ĐẦY ĐỦ các từ khóa ghép lại (Bất kể vị trí)
        if job_words:
            match_all_title = True
            for w in job_words:
                if len(w) <= 3:
                    if not re.search(rf'\b{re.escape(w)}\b', title): match_all_title = False; break
                else:
                    if w not in title: match_all_title = False; break
            if match_all_title: score += 5000
        
        # Chấm điểm từ khóa phụ
        for kw in query_keywords:
            if len(kw) <= 3:
                if re.search(rf'\b{re.escape(kw)}\b', title): score += 100
                elif re.search(rf'\b{re.escape(kw)}\b', chunk): score += 10
            else:
                if kw in title: score += 100
                elif kw in chunk: score += 10
        return score

    df_jobs['lexical_score'] = df_jobs.apply(compute_lexical_score, axis=1)
    
    if job_kw or query_keywords: df_jobs = df_jobs[df_jobs['lexical_score'] > 0]
    if df_jobs.empty: return pd.DataFrame(), fallback_triggered, requested_loc

    # 4. RRF RANKING DUNG HỢP KẾT QUẢ
    df_jobs['vector_rank'] = df_jobs['vector_distance'].rank(ascending=True)
    df_jobs['lexical_rank'] = df_jobs['lexical_score'].rank(ascending=False)
    df_jobs['rrf_score'] = (1 / (60 + df_jobs['vector_rank'])) + (1 / (60 + df_jobs['lexical_rank']))
    
    return df_jobs.sort_values(by='rrf_score', ascending=False).head(top_k), fallback_triggered, requested_loc

def generate_llm_response(user_query, jobs_df, cv_text, chat_history, fallback_triggered, requested_loc):
    context_str = ""
    for idx, row in jobs_df.iterrows():
        desc = str(row['chunk_text']).replace('\n', ' ')[:800]
        context_str += f"- Tên Job: {row['job_title']} | Công ty: {row['company_name']} | Lương: {row['salary_text']} | Link: {row['job_url']} | Chi tiết: {desc}...\n\n"
    
    system_prompt = f"Bạn là AI Headhunter của VietnamWorks.\n[DỮ LIỆU TỪ HỆ THỐNG]:\n{context_str if context_str else 'Trống.'}\n"
    if fallback_triggered:
        system_prompt += f"\n⚠️ LỖI: HIỆN KHÔNG CÓ CÔNG VIỆC TẠI ({requested_loc.upper()}). Các công việc ở trên nằm ở khu vực KHÁC.\n"

    system_prompt += """[KỶ LUẬT THÉP]:
1. TRUNG THỰC: CHỈ phân tích các công việc trong [DỮ LIỆU]. KHÔNG bịa công ty hay địa điểm.
2. NẾU LỖI ĐỊA ĐIỂM: Bắt buộc thông báo: "Rất tiếc hệ thống không có vị trí này tại khu vực bạn yêu cầu. Tuy nhiên, có cơ hội tại khu vực khác...".
3. ĐÍNH LINK: Bắt buộc dùng Markdown: **[Tên Công Việc](Link_Của_Job)**.
4. Phân tích đối chiếu chuyên sâu.
"""
    if cv_text: system_prompt += f"\n[CV ỨNG VIÊN]:\n{cv_text[:1500]}\n"

    messages = [{'role': 'system', 'content': system_prompt}] + [{'role': m['role'], 'content': m['content']} for m in chat_history[-6:]] + [{'role': 'user', 'content': user_query}]
    client = Client(host="https://ollama.com", headers={'Authorization': 'Bearer ' + os.environ.get('OLLAMA_API_KEY', '')})
    try:
        full_response = ""
        for part in client.chat(model=CLOUD_LLM_MODEL, messages=messages, stream=True): full_response += part['message']['content']
        return full_response
    except Exception as e: return f"⚠️ Lỗi API AI: {e}"