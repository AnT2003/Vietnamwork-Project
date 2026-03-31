# ==========================================
# Thực hiện Transform & Chuẩn hóa bằng SQL
# Nhiệm vụ: Chứa toàn bộ các câu lệnh SQL phục vụ cho quá trình ETL 
# Data Quality, Logging và truy xuất dữ liệu cho AI.
# ==========================================

# ==============================================================================
# 1. NHÓM LỆNH LOGGING (AUDIT TRAIL)
# Dùng để theo dõi lịch sử chạy Pipeline, lưu trạng thái Thành công/Thất bại
# ==============================================================================
SQL_LOG_START = "INSERT INTO audit.etl_log (pipeline_name, status) VALUES (:pipeline_name, 'RUNNING') RETURNING log_id;"
SQL_LOG_SUCCESS = "UPDATE audit.etl_log SET status = 'SUCCESS', end_time = CURRENT_TIMESTAMP, records_processed = :records WHERE log_id = :log_id;"
SQL_LOG_FAIL = "UPDATE audit.etl_log SET status = 'FAILED', end_time = CURRENT_TIMESTAMP, error_message = :err_msg WHERE log_id = :log_id;"

# ==============================================================================
# 2. LỆNH LÀM SẠCH VÙNG ĐỆM (STAGING)
# Xóa trắng (Truncate) các bảng tạm trước khi chạy Crawler mới để không bị lẫn lộn data cũ
# ==============================================================================
SQL_TRUNCATE_STAGING = "TRUNCATE TABLE staging.raw_fact_job_postings, staging.raw_dim_job_details, staging.raw_dim_companies, staging.raw_dim_industries, staging.raw_dim_locations, staging.raw_dim_skills;"

# ==============================================================================
# 3. SIÊU TRUY VẤN: TRANSFORM & LOAD (TỪ STAGING SANG DATA WAREHOUSE)
# ==============================================================================
SQL_TRANSFORM_LOAD_DWH = """
    -- 3.1 Nạp dữ liệu vào bảng Dimension Đơn (Công ty, Ngành nghề)
    -- Dùng ON CONFLICT DO NOTHING để bỏ qua tự động nếu ID đã tồn tại (Chống Duplicate)
    INSERT INTO dwh.dim_companies 
    SELECT DISTINCT CAST(REPLACE(NULLIF(company_id, ''), '.0', '') AS BIGINT), company_name, description, logo_url, profile_url 
    FROM staging.raw_dim_companies WHERE company_id != '' ON CONFLICT (company_id) DO NOTHING;
    
    INSERT INTO dwh.dim_industries 
    SELECT DISTINCT CAST(REPLACE(NULLIF(industry_id, ''), '.0', '') AS BIGINT), industry_name 
    FROM staging.raw_dim_industries WHERE industry_id != '' ON CONFLICT (industry_id) DO NOTHING;
    
    -- 3.2 Nạp dữ liệu vào bảng Dimension Chi tiết Công việc (Chứa logic Transform cực nặng)
    INSERT INTO dwh.dim_job_details (
        job_id, job_title, job_url, salary_text, salary_numeric, job_level, 
        posted_date, expiry_date, years_of_experience, 
        job_description, job_requirements, job_benefits
    )
    SELECT 
        job_id, job_title, job_url, salary_text, 
        
        -- LOGIC 2: Chuẩn hóa ngưỡng lương sau khi tính toán
        -- Loại bỏ các mức lương ảo (> 500 triệu), quy đổi mức lương bị gõ thiếu số 0 (ví dụ 15-20 -> 15tr-20tr)
        CASE 
            WHEN raw_salary_numeric > 500000000 THEN 0 
            WHEN raw_salary_numeric > 0 AND raw_salary_numeric < 1000 THEN raw_salary_numeric * 1000000 
            WHEN raw_salary_numeric >= 1000 AND raw_salary_numeric < 500000 THEN 0 
            ELSE raw_salary_numeric
        END AS salary_numeric,
        
        job_level, posted_date, expiry_date, years_of_experience, 
        job_description, job_requirements, job_benefits 
    FROM (
        SELECT DISTINCT ON (cleaned_raw.job_id) 
            cleaned_raw.job_id, job_title, job_url, salary_text, 
            
            -- LOGIC 1: Thuật toán Bóc tách và Quy đổi tiền tệ từ chuỗi (Text) sang Số (Numeric)
            CAST(
                CASE 
                    -- Nếu là lương thỏa thuận -> Gán bằng 0
                    WHEN LOWER(salary_text) LIKE '%thương lượng%' THEN 0
                    ELSE 
                        -- Nếu là khoảng lương (10 - 20 triệu) -> Lấy (Min + Max) / 2 để ra số trung bình
                        COALESCE(
                            (
                                CAST(NULLIF(SUBSTRING(REPLACE(salary_text, ',', '') FROM '([0-9]+\\.?[0-9]*)'), '') AS NUMERIC) 
                                + 
                                COALESCE(
                                    CAST(NULLIF(SUBSTRING(REPLACE(salary_text, ',', '') FROM '[0-9]+\\.?[0-9]*[^0-9]+([0-9]+\\.?[0-9]*)'), '') AS NUMERIC),
                                    CAST(NULLIF(SUBSTRING(REPLACE(salary_text, ',', '') FROM '([0-9]+\\.?[0-9]*)'), '') AS NUMERIC)
                                )
                            ) / 2 
                            -- Nhân với tỷ giá (USD -> VND) hoặc đơn vị (Triệu)
                            * CASE 
                                WHEN LOWER(salary_text) LIKE '%usd%' OR salary_text LIKE '%$%' THEN 25000
                                WHEN LOWER(salary_text) LIKE '%tr%' OR LOWER(salary_text) LIKE '%triệu%' THEN 1000000
                                ELSE 1 
                            END
                            -- Chia cho 12 nếu là lương theo năm (Yearly)
                            / 
                            CASE 
                                WHEN LOWER(salary_text) LIKE '%năm%' OR LOWER(salary_text) LIKE '%year%' THEN 12
                                ELSE 1 
                            END
                        , 0)
                END AS BIGINT
            ) AS raw_salary_numeric,
            
            job_level, posted_date, expiry_date, 
            
            -- LOGIC 3: Trích xuất số năm kinh nghiệm từ chuỗi (Ví dụ: "1 - 3 năm" -> lấy số)
            CASE 
                WHEN LOWER(years_of_experience) LIKE '%không%' THEN 0
                WHEN REGEXP_REPLACE(years_of_experience, '[^0-9]', '', 'g') <> '' 
                    THEN CAST(REGEXP_REPLACE(years_of_experience, '[^0-9]', '', 'g') AS INTEGER)
                ELSE 0 
            END AS years_of_experience,
            
            job_description, job_requirements, job_benefits 
        FROM (
            SELECT 
                CAST(REPLACE(NULLIF(job_id, ''), '.0', '') AS BIGINT) AS job_id, 
                job_title, job_url, salary_text, job_level, 
                -- Chuẩn hóa định dạng chuỗi ngày tháng (Date)
                CASE 
                    WHEN posted_date LIKE '%/%' THEN TO_DATE(posted_date, 'DD/MM/YYYY') 
                    WHEN NULLIF(posted_date, '') IS NOT NULL THEN CAST(posted_date AS DATE) 
                    ELSE NULL 
                END AS posted_date, 
                CASE 
                    WHEN expiry_date LIKE '%/%' THEN TO_DATE(expiry_date, 'DD/MM/YYYY') 
                    WHEN NULLIF(expiry_date, '') IS NOT NULL THEN CAST(expiry_date AS DATE) 
                    ELSE NULL 
                END AS expiry_date, 
                years_of_experience, 
                -- LOGIC 4: Sử dụng REGEXP_REPLACE để quét và dọn sạch toàn bộ các thẻ HTML (<p>, <ul>...) trong văn bản
                TRIM(REGEXP_REPLACE(job_description, '<[^>]*>', ' ', 'g')) AS job_description, 
                TRIM(REGEXP_REPLACE(job_requirements, '<[^>]*>', ' ', 'g')) AS job_requirements, 
                TRIM(REGEXP_REPLACE(job_benefits, '<[^>]*>', ' ', 'g')) AS job_benefits
            FROM staging.raw_dim_job_details WHERE job_id != ''
        ) cleaned_raw
    ) calculated_data
    -- UPSERT: Nếu Job ID đã tồn tại thì tự động cập nhật lại mức lương và kinh nghiệm mới nhất
    ON CONFLICT (job_id) DO UPDATE SET 
        salary_numeric = EXCLUDED.salary_numeric,
        years_of_experience = EXCLUDED.years_of_experience;
    
    -- 3.3 Nạp dữ liệu vào các Bảng nhánh Đa trị (Bridge Tables)
    INSERT INTO dwh.dim_locations 
    SELECT DISTINCT CAST(REPLACE(NULLIF(job_id, ''), '.0', '') AS BIGINT), location_name 
    FROM staging.raw_dim_locations WHERE job_id != '' AND location_name != '' ON CONFLICT (job_id, location_name) DO NOTHING;
    
    INSERT INTO dwh.dim_skills 
    SELECT DISTINCT CAST(REPLACE(NULLIF(job_id, ''), '.0', '') AS BIGINT), skill_name 
    FROM staging.raw_dim_skills WHERE job_id != '' AND skill_name != '' ON CONFLICT (job_id, skill_name) DO NOTHING;

    INSERT INTO dwh.dim_categories 
    SELECT DISTINCT CAST(REPLACE(NULLIF(job_id, ''), '.0', '') AS BIGINT), category_name 
    FROM staging.raw_dim_categories WHERE job_id != '' AND category_name != '' ON CONFLICT (job_id, category_name) DO NOTHING;
    
    -- 3.4 Nạp dữ liệu vào Bảng Fact Trung tâm (Kết nối tất cả các ID lại)
    INSERT INTO dwh.fact_job_postings 
    SELECT CAST(REPLACE(NULLIF(job_id, ''), '.0', '') AS BIGINT), CAST(REPLACE(NULLIF(company_id, ''), '.0', '') AS BIGINT), CAST(REPLACE(NULLIF(industry_id, ''), '.0', '') AS BIGINT), CAST(NULLIF(crawled_at, '') AS TIMESTAMP) 
    FROM staging.raw_fact_job_postings WHERE job_id != '' AND company_id != '' AND industry_id != '' ON CONFLICT (job_id) DO NOTHING;
"""

# ==============================================================================
# 4. NHÓM LỆNH DATA QUALITY (Chỉ dùng riêng nếu không chạy rules Python)
# ==============================================================================
SQL_VALIDATE_NULL = "SELECT COUNT(*) FROM dwh.fact_job_postings WHERE job_id IS NULL;"
SQL_VALIDATE_COUNT = "SELECT COUNT(*) FROM dwh.fact_job_postings;"

# ==============================================================================
# 5. TRUY VẤN CUNG CẤP DỮ LIỆU CHO AI VECTORIZATION
# ==============================================================================
SQL_FETCH_JOBS_FOR_EMBEDDING = """
    -- Mục đích: Gom toàn bộ thông tin của 1 Job nằm rải rác ở nhiều bảng thành 1 hàng duy nhất (Wide Table)
    SELECT 
        f.job_id, 
        d.job_title, 
        c.company_name, 
        c.description AS company_description, 
        i.industry_name, 
        d.salary_text, 
        d.job_level,
        d.years_of_experience, 
        d.posted_date,
        d.expiry_date,
        d.job_description, 
        d.job_requirements,
        d.job_benefits,
        COALESCE(loc.locations, 'Không xác định') AS locations,
        COALESCE(sk.skills, 'Không xác định') AS skills
    FROM dwh.fact_job_postings f
    JOIN dwh.dim_job_details d ON f.job_id = d.job_id
    JOIN dwh.dim_companies c ON f.company_id = c.company_id
    JOIN dwh.dim_industries i ON f.industry_id = i.industry_id
    -- Kỹ thuật STRING_AGG: Nhóm nhiều dòng Locations/Skills của 1 Job thành 1 chuỗi dài (VD: "Python, Java, SQL")
    LEFT JOIN (
        SELECT job_id, STRING_AGG(location_name, ', ') AS locations 
        FROM dwh.dim_locations GROUP BY job_id
    ) loc ON f.job_id = loc.job_id
    LEFT JOIN (
        SELECT job_id, STRING_AGG(skill_name, ', ') AS skills 
        FROM dwh.dim_skills GROUP BY job_id
    ) sk ON f.job_id = sk.job_id
    
    -- Kỹ thuật DELTA LOAD: Kết nối (LEFT JOIN) với bảng Vector. Chỉ lấy những Job chưa từng có mặt 
    -- trong bảng Vector (v.job_id IS NULL). Từ đó AI chỉ tốn công xử lý các Job mới cào ngày hôm nay!
    LEFT JOIN vector_dwh.dim_job_vectors v ON f.job_id = v.job_id
    WHERE v.job_id IS NULL;
"""

# Lệnh Upsert đẩy kết quả Vector (Array 384 chiều) vào Database
SQL_UPSERT_VECTOR = "INSERT INTO vector_dwh.dim_job_vectors (job_id, chunk_text, embedding) VALUES (:job_id, :chunk_text, :embedding) ON CONFLICT (job_id) DO UPDATE SET chunk_text = EXCLUDED.chunk_text, embedding = EXCLUDED.embedding;"