import pandas as pd
from sqlalchemy import text
import re
from collections import Counter
from core_engine.ai_engine import engine

# ==============================================================================
# Lấy danh sách giá trị duy nhất (unique) cho các hộp thoại lọc (Filter)
# ==============================================================================
def get_filter_options():
    with engine.connect() as conn:
        # Truy vấn lấy danh sách Ngành nghề (Industry), Danh mục (Category), Cấp bậc (Level)
        res_ind = conn.execute(text("SELECT DISTINCT industry_name FROM dwh.dim_industries WHERE industry_name IS NOT NULL")).fetchall()
        res_cat = conn.execute(text("SELECT DISTINCT category_name FROM dwh.dim_categories WHERE category_name IS NOT NULL")).fetchall()
        res_lvl = conn.execute(text("SELECT DISTINCT job_level FROM dwh.dim_job_details WHERE job_level IS NOT NULL")).fetchall()
        # Truy vấn lấy danh sách Địa điểm (Location)
        res_loc = conn.execute(text("SELECT DISTINCT location_name FROM dwh.dim_locations WHERE location_name IS NOT NULL")).fetchall()
    
    # Trả về các danh sách, luôn gắn giá trị "All" (Tất cả) lên vị trí đầu tiên
    return ["All"] + [r[0] for r in res_ind], ["All"] + [r[0] for r in res_cat], ["All"] + [r[0] for r in res_lvl], ["All"] + [r[0] for r in res_loc]

# ==============================================================================
# Xử lý truy vấn SQL động và trả về toàn bộ dữ liệu thống kê cho Dashboard
# ==============================================================================
def load_dashboard_data_json(ind_f, cat_f, lvl_f, loc_f="All"):
    # Khởi tạo điều kiện WHERE cơ bản ("1=1" luôn đúng) và dictionary chứa tham số truy vấn
    wheres, p = ["1=1"], {}    
    # Bổ sung điều kiện lọc cơ bản nếu người dùng có chọn filter (khác "All")
    if ind_f != "All": wheres.append("i.industry_name = :ind"); p['ind'] = ind_f
    if cat_f != "All": wheres.append("cat.category_name = :cat"); p['cat'] = cat_f
    if lvl_f != "All": wheres.append("d.job_level = :lvl"); p['lvl'] = lvl_f
    # Lọc Location bằng Subquery (Truy vấn con): Kỹ thuật này giúp tránh việc Join trực tiếp 
    # với bảng dim_locations gây bùng nổ dữ liệu (Duplicate) do tính chất đa trị của Location.
    if loc_f != "All": 
        wheres.append("f.job_id IN (SELECT job_id FROM dwh.dim_locations WHERE location_name = :loc)")
        p['loc'] = loc_f    
    # Ghép các mảng điều kiện lại thành chuỗi WHERE hoàn chỉnh
    ws = " AND ".join(wheres)   
    # Cấu trúc JOIN cốt lõi dùng chung cho tất cả các truy vấn biểu đồ phía dưới
    bj = "FROM dwh.fact_job_postings f JOIN dwh.dim_job_details d ON f.job_id = d.job_id LEFT JOIN dwh.dim_industries i ON f.industry_id = i.industry_id LEFT JOIN dwh.dim_categories cat ON f.job_id = cat.job_id"  
    # Cấu trúc khung JSON trả về mặc định, đảm bảo giao diện không bị sập nếu SQL bị lỗi
    data = {
        "kpi": {"total_jobs": "0", "total_companies": "0", "avg_salary": "N/A", "avg_exp": "N/A"},
        "bar_skills": {"labels": [], "data": []},
        "pie_levels": {"labels": [], "data": []},
        "bar_companies": {"labels": [], "data": []},
        "bar_salaries": {"labels": [], "data": []},
        "mix_exp": {"labels": [], "jobs": [], "salary": []},
        "treemap": [],
        "pie_locations": {"labels": [], "data": []},
        "word_cloud": []
    }
    
    try:
        with engine.connect() as conn:
            # Hàm nội bộ (helper function) để tái sử dụng, giúp thực thi SQL an toàn có binding tham số (p)
            def execute_sql(query):
                return conn.execute(text(query), p) if p else conn.execute(text(query))

            # 1. TÍNH TOÁN CÁC CHỈ SỐ KPI TỔNG QUAN (4 thẻ trên cùng)
            try:
                m = execute_sql(f"SELECT COUNT(DISTINCT f.job_id), COUNT(DISTINCT f.company_id), AVG(NULLIF(d.salary_numeric, 0)), AVG(d.years_of_experience) {bj} WHERE {ws}").fetchone()
                data["kpi"]["total_jobs"] = f"{int(m[0]):,}" if m and m[0] else "0"
                data["kpi"]["total_companies"] = f"{int(m[1]):,}" if m and m[1] else "0"
                data["kpi"]["avg_salary"] = f"{float(m[2])/1000000:.1f} M" if m and m[2] else "N/A"
                data["kpi"]["avg_exp"] = f"{float(m[3]):.1f} Years" if m and m[3] is not None else "0 Years"
            except Exception as e: print(f"Lỗi KPI: {e}")

            # 2. BIỂU ĐỒ BAR: TOP 10 KỸ NĂNG ĐƯỢC YÊU CẦU NHIỀU NHẤT
            try:
                s = pd.DataFrame(execute_sql(f"SELECT s.skill_name, COUNT(DISTINCT f.job_id) as count {bj} JOIN dwh.dim_skills s ON f.job_id = s.job_id WHERE {ws} GROUP BY s.skill_name ORDER BY count DESC LIMIT 10").fetchall(), columns=['skill_name', 'count'])
                if not s.empty: data["bar_skills"] = {"labels": s['skill_name'].astype(str).tolist(), "data": [int(x) for x in s['count'].tolist()]}
            except Exception as e: print(f"Lỗi Skills: {e}")

            # 3. BIỂU ĐỒ PIE: PHÂN BỔ CÔNG VIỆC THEO CẤP BẬC YÊU CẦU (LEVEL)
            try:
                l = pd.DataFrame(execute_sql(f"SELECT d.job_level, COUNT(DISTINCT f.job_id) as count {bj} WHERE {ws} GROUP BY d.job_level ORDER BY count DESC").fetchall(), columns=['job_level', 'count'])
                if not l.empty: data["pie_levels"] = {"labels": l['job_level'].astype(str).tolist(), "data": [int(x) for x in l['count'].tolist()]}
            except Exception as e: print(f"Lỗi Levels: {e}")

            # 4. BIỂU ĐỒ BAR: TOP 10 CÔNG TY ĐĂNG TUYỂN NHIỀU CÔNG VIỆC NHẤT
            try:
                c = pd.DataFrame(execute_sql(f"SELECT c.company_name, COUNT(DISTINCT f.job_id) as count {bj} JOIN dwh.dim_companies c ON f.company_id = c.company_id WHERE {ws} GROUP BY c.company_name ORDER BY count DESC LIMIT 10").fetchall(), columns=['company_name', 'count'])
                if not c.empty: data["bar_companies"] = {"labels": c['company_name'].astype(str).tolist(), "data": [int(x) for x in c['count'].tolist()]}
            except Exception as e: print(f"Lỗi Companies: {e}")

            # 5. BIỂU ĐỒ BAR: TOP 10 DANH MỤC CÔNG VIỆC (CATEGORY) CÓ MỨC LƯƠNG TRUNG BÌNH CAO NHẤT
            try:
                cs = pd.DataFrame(execute_sql(f"SELECT cat.category_name, AVG(NULLIF(d.salary_numeric, 0)) as avg_salary {bj} WHERE {ws} AND d.salary_numeric > 0 AND cat.category_name IS NOT NULL GROUP BY cat.category_name ORDER BY avg_salary DESC LIMIT 10").fetchall(), columns=['category_name', 'avg_salary'])
                if not cs.empty: data["bar_salaries"] = {"labels": cs['category_name'].astype(str).tolist(), "data": [round(float(x)/1000000, 1) for x in cs['avg_salary'].tolist()]}
            except Exception as e: print(f"Lỗi Salaries: {e}")

            # 6. BIỂU ĐỒ KẾT HỢP (MIX): TƯƠNG QUAN GIỮA SỐ NĂM KINH NGHIỆM VÀ MỨC LƯƠNG (Phân tích dưới 10 năm)
            try:
                se = pd.DataFrame(execute_sql(f"SELECT d.years_of_experience, AVG(NULLIF(d.salary_numeric, 0)) as avg_salary, COUNT(DISTINCT f.job_id) as count {bj} WHERE {ws} AND d.salary_numeric > 0 AND d.years_of_experience <= 10 GROUP BY d.years_of_experience ORDER BY d.years_of_experience").fetchall(), columns=['years_of_experience', 'avg_salary', 'count'])
                if not se.empty: data["mix_exp"] = {"labels": se['years_of_experience'].astype(str).tolist(), "jobs": [int(x) for x in se['count'].tolist()], "salary": [round(float(x)/1000000, 1) if pd.notnull(x) else 0.0 for x in se['avg_salary'].tolist()]}
            except Exception as e: print(f"Lỗi Exp: {e}")

            # 7. BIỂU ĐỒ TREEMAP: HIỂN THỊ CẤU TRÚC THỊ TRƯỜNG THEO NGÀNH (INDUSTRY) VÀ CÁC DANH MỤC CON
            try:
                tm = pd.DataFrame(execute_sql(f"SELECT i.industry_name, cat.category_name, COUNT(DISTINCT f.job_id) as count {bj} WHERE {ws} AND i.industry_name IS NOT NULL AND cat.category_name IS NOT NULL GROUP BY i.industry_name, cat.category_name").fetchall(), columns=['industry_name', 'category_name', 'count'])
                if not tm.empty:
                    # Logic: Tìm ra 5 Ngành nghề lớn nhất, sau đó lấy tối đa 2 Danh mục con phổ biến nhất ở mỗi ngành
                    top_ind = tm.groupby('industry_name')['count'].sum().nlargest(5).index
                    tm = tm[tm['industry_name'].isin(top_ind)]
                    tm = tm.sort_values(['industry_name', 'count'], ascending=[True, False])
                    tm['rank'] = tm.groupby('industry_name').cumcount()
                    tm = tm[tm['rank'] < 3]
                    
                    # Convert sang format mà ChartJS Treemap có thể đọc được
                    data["treemap"] = [{"industry": str(row['industry_name']), "category": str(row['category_name']), "value": int(row['count'])} for _, row in tm.iterrows()]
            except Exception as e: print(f"Lỗi Treemap: {e}")

            # 8. BIỂU ĐỒ PIE: TOP 5 ĐỊA ĐIỂM (LOCATION) CÓ SỐ LƯỢNG JOB NHIỀU NHẤT
            try:
                loc = pd.DataFrame(execute_sql(f"SELECT loc.location_name as location, COUNT(DISTINCT f.job_id) as count {bj} JOIN dwh.dim_locations loc ON f.job_id = loc.job_id WHERE {ws} AND loc.location_name IS NOT NULL GROUP BY loc.location_name ORDER BY count DESC LIMIT 5").fetchall(), columns=['location', 'count'])
                if not loc.empty: data["pie_locations"] = {"labels": loc['location'].astype(str).tolist(), "data": [int(x) for x in loc['count'].tolist()]}
            except Exception as e: print(f"Lỗi Location: {e}")

            # 9. WORD CLOUD (ĐÁM MÂY TỪ KHÓA): PHÂN TÍCH TẦN SUẤT TỪ KHÓA TRONG YÊU CẦU CÔNG VIỆC
            try:
                # Lấy ngẫu nhiên 100 mô tả công việc (job_requirements)
                req_data = execute_sql(f"SELECT d.job_requirements {bj} WHERE {ws} AND d.job_requirements IS NOT NULL LIMIT 100").fetchall()
                if req_data:
                    # Loại bỏ thẻ HTML bằng Regex (re.sub) và trích xuất các từ (re.findall)
                    text_corpus = " ".join([str(r[0]) for r in req_data if r[0]])
                    text_corpus = re.sub(r'<[^>]+>', ' ', text_corpus) 
                    words = re.findall(r'\b[a-zA-Z_]{3,20}\b', text_corpus.lower())
                    
                    # Từ điển Stop Words: Loại bỏ các từ nối, từ phổ thông không mang ý nghĩa chuyên ngành
                    stop_words = {
                        'and', 'the', 'for', 'with', 'you', 'are', 'will', 'have', 'from', 'our', 'your', 'can', 'that', 'this', 'all', 'any', 'not', 'but', 'also', 'has', 'was', 'were', 'they', 'their', 'them', 'what', 'which', 'who', 'how', 'why', 'where', 'when', 'there', 'here', 'out', 'into', 'over', 'under', 'some', 'such', 'only', 'own', 'same', 'other', 'another', 'each', 'both', 'much', 'many', 'more', 'most', 'few', 'fewer', 'less', 'least', 'very', 'too', 'quite', 'rather', 'just', 'so', 'enough', 'even', 'still', 'almost', 'always', 'never', 'often', 'sometimes', 'usually', 'generally', 'frequently', 'rarely', 'seldom', 'hardly', 'scarcely', 'already', 'yet', 'soon', 'late', 'early', 'now', 'then', 'before', 'after', 'while', 'during', 'until', 'since', 'because', 'unless', 'although', 'though', 'provided', 'providing', 'supposing', 'whether', 'either', 'neither', 'than', 'however', 'nevertheless', 'nonetheless', 'notwithstanding', 'instead', 'whereas', 'otherwise', 'conversely', 'experience', 'skills', 'work', 'ability', 'knowledge', 'team', 'strong', 'good', 'must', 'required', 'requirements', 'years', 'understanding', 'working', 'development', 'business', 'support', 'management', 'design', 'related', 'degree', 'candidate', 'company', 'projects', 'data', 'using', 'least', 'excellent', 'environment', 'project', 'new', 'system', 'build', 'software', 'equivalent', 'communication', 'level', 'including', 'technology', 'technical', 'systems', 'based', 'time', 'process', 'job', 'role', 'responsibilities', 'duties', 'position', 'benefits', 'salary', 'opportunities', 'career', 'professional', 'training', 'health', 'insurance', 'holiday', 'bonus', 'annual', 'leave', 'paid', 'day', 'days', 'week', 'month', 'year', 'performance', 'review', 'evaluation', 'promotion', 'growth', 'culture', 'values', 'vision', 'mission', 'goal', 'objective', 'strategy', 'plan', 'execution', 'implementation', 'operation', 'maintenance', 'service', 'quality', 'assurance', 'control', 'testing', 'debugging', 'troubleshooting', 'issue', 'problem', 'solution', 'resolution', 'improvement', 'enhancement', 'optimization', 'efficiency', 'effectiveness', 'productivity', 'innovation', 'creativity', 'idea', 'concept', 'architecture', 'structure', 'framework', 'pattern', 'principle', 'practice', 'standard', 'guideline', 'policy', 'procedure', 'methodology', 'method', 'technique', 'tool', 'instrument', 'equipment', 'device', 'machine', 'hardware', 'application', 'platform', 'infrastructure', 'network', 'database', 'server', 'client', 'user', 'customer', 'partner', 'vendor', 'supplier', 'stakeholder', 'shareholder', 'investor', 'board', 'director', 'executive', 'manager', 'leader', 'supervisor', 'coordinator', 'group', 'department', 'division', 'unit', 'branch', 'office', 'facility', 'site', 'location', 'region', 'area', 'zone', 'country', 'state', 'city', 'town', 'village', 'community', 'society', 'market', 'industry', 'sector', 'field', 'domain', 'discipline', 'profession', 'occupation', 'title', 'duty', 'responsibility', 'task', 'assignment', 'program', 'campaign', 'initiative', 'activity', 'event', 'function', 'rule', 'regulation', 'law', 'requirement', 'specification', 'instruction', 'direction', 'manual', 'guide', 'document', 'record', 'report', 'form', 'template', 'information', 'insight', 'intelligence', 'wisdom', 'comprehension', 'awareness', 'familiarity',
                        'trong', 'và', 'với', 'kinh', 'nghiệm', 'có', 'kỹ', 'năng', 'việc', 'làm', 'của', 'các', 'yêu', 'cầu', 'thực', 'hiện', 'tham', 'gia', 'chịu', 'trách', 'nhiệm', 'những', 'được', 'hoặc', 'tốt', 'biết', 'khác', 'phát', 'triển', 'hệ', 'thống', 'thiết', 'kế', 'đảm', 'bảo', 'quản', 'lý', 'dự', 'án', 'sử', 'dụng', 'hiểu', 'kiến', 'thức', 'chuyên', 'môn', 'môi', 'trường', 'công', 'ty', 'khách', 'hàng', 'hỗ', 'trợ', 'sản', 'phẩm', 'từ', 'trở', 'lên', 'ít', 'nhất', 'năm', 'đại', 'học', 'cao', 'đẳng', 'ngành', 'liên', 'quan', 'tương', 'đương', 'tiếng', 'anh', 'giao', 'tiếp', 'thành', 'thạo', 'khả', 'độc', 'lập', 'áp', 'lực', 'động', 'sáng', 'tạo', 'trung', 'cẩn', 'thận', 'nhiệt', 'tình', 'chủ', 'tinh', 'thần', 'mong', 'muốn', 'gắn', 'bó', 'lâu', 'dài', 'chế', 'độ', 'đãi', 'ngộ', 'phúc', 'lợi', 'bảo', 'hiểm', 'thưởng', 'lễ', 'tết', 'nghỉ', 'phép', 'du', 'lịch', 'đào', 'tạo', 'thăng', 'tiến', 'cơ', 'hội', 'thu', 'nhập', 'hấp', 'dẫn', 'cạnh', 'tranh', 'thỏa', 'thuận', 'theo', 'lực', 'phù', 'hợp', 'đầy', 'đủ', 'quy', 'định', 'pháp', 'luật', 'nhà', 'nước', 'việt', 'nam', 'bằng', 'cấp', 'chứng', 'chỉ', 'tin', 'văn', 'phòng', 'phần', 'mềm', 'cụ', 'ứng', 'cơ', 'bản', 'nâng', 'nắm', 'vững', 'rõ', 'am', 'sâu', 'rộng', 'nhạy', 'bén', 'bắt', 'cập', 'nhật', 'xu', 'hướng', 'thị', 'mới', 'tiên', 'hiện', 'đại', 'quốc', 'tế', 'toàn', 'cầu', 'đa', 'ngoài', 'nhập', 'khẩu', 'xuất', 'thương', 'mại', 'dịch', 'vụ', 'kinh', 'doanh', 'bán', 'tiếp', 'thị', 'truyền', 'thông', 'quảng', 'cáo', 'sự', 'kiện', 'nhân', 'sự', 'hành', 'chính', 'kế', 'toán', 'tài', 'chính', 'kiểm', 'toán', 'thuế', 'ngân', 'tín', 'đầu', 'tư', 'chứng', 'khoán', 'bất', 'động', 'sản', 'xây', 'dựng', 'kiến', 'trúc', 'nội', 'thất', 'ngoại', 'cơ', 'khí', 'chế', 'tạo', 'tự', 'động', 'hóa', 'điện', 'tử', 'viễn', 'thông', 'nghệ', 'cứng', 'mạng', 'mật', 'an', 'toàn', 'liệu'
                    }
                    # Lọc từ khóa, đếm tần suất và trả về 50 từ xuất hiện nhiều nhất
                    filtered_words = [w for w in words if w not in stop_words and len(w) > 3]
                    most_common = Counter(filtered_words).most_common(50)
                    data["word_cloud"] = [{"key": k.capitalize(), "value": v} for k, v in most_common]
            except Exception as e: print(f"Lỗi Word Cloud: {e}")
            return data
            
    except Exception as e:
        # Bắt lỗi cấp độ hệ thống (ví dụ sập DB, mất kết nối)
        print(f"LỖI TỔNG DASHBOARD: {e}")
        return data