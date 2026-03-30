# ==========================================
# FILE 2: sql_queries.py (Thực hiện Transform & Chuẩn hóa bằng SQL)
# ==========================================
SQL_LOG_START = "INSERT INTO audit.etl_log (pipeline_name, status) VALUES (:pipeline_name, 'RUNNING') RETURNING log_id;"
SQL_LOG_SUCCESS = "UPDATE audit.etl_log SET status = 'SUCCESS', end_time = CURRENT_TIMESTAMP, records_processed = :records WHERE log_id = :log_id;"
SQL_LOG_FAIL = "UPDATE audit.etl_log SET status = 'FAILED', end_time = CURRENT_TIMESTAMP, error_message = :err_msg WHERE log_id = :log_id;"

SQL_TRUNCATE_STAGING = "TRUNCATE TABLE staging.raw_fact_job_postings, staging.raw_dim_job_details, staging.raw_dim_companies, staging.raw_dim_industries, staging.raw_dim_locations, staging.raw_dim_skills;"

# TRANSFORM TRONG SQL: Dùng REGEXP_REPLACE để loại bỏ thẻ HTML và dọn rác
SQL_TRANSFORM_LOAD_DWH = """
    INSERT INTO dwh.dim_companies 
    SELECT DISTINCT CAST(REPLACE(NULLIF(company_id, ''), '.0', '') AS BIGINT), company_name, description, logo_url, profile_url 
    FROM staging.raw_dim_companies WHERE company_id != '' ON CONFLICT (company_id) DO NOTHING;
    
    INSERT INTO dwh.dim_industries 
    SELECT DISTINCT CAST(REPLACE(NULLIF(industry_id, ''), '.0', '') AS BIGINT), industry_name 
    FROM staging.raw_dim_industries WHERE industry_id != '' ON CONFLICT (industry_id) DO NOTHING;
    
    INSERT INTO dwh.dim_job_details (
        job_id, job_title, job_url, salary_text, salary_numeric, job_level, 
        view_count, posted_date, expiry_date, years_of_experience, 
        job_description, job_requirements, job_benefits
    )
    SELECT 
        job_id, job_title, job_url, salary_text, 
        
        CASE 
            WHEN raw_salary_numeric > 500000000 THEN 0 
            WHEN raw_salary_numeric > 0 AND raw_salary_numeric < 1000 THEN raw_salary_numeric * 1000000 
            WHEN raw_salary_numeric >= 1000 AND raw_salary_numeric < 500000 THEN 0 
            ELSE raw_salary_numeric
        END AS salary_numeric,
        
        job_level, view_count, posted_date, expiry_date, years_of_experience, 
        job_description, job_requirements, job_benefits 
    FROM (
        SELECT DISTINCT ON (cleaned_raw.job_id) 
            cleaned_raw.job_id, job_title, job_url, salary_text, 
            
            CAST(
                CASE 
                    WHEN LOWER(salary_text) LIKE '%thương lượng%' THEN 0
                    ELSE 
                        COALESCE(
                            (
                                CAST(NULLIF(SUBSTRING(REPLACE(salary_text, ',', '') FROM '([0-9]+\\.?[0-9]*)'), '') AS NUMERIC) 
                                + 
                                COALESCE(
                                    CAST(NULLIF(SUBSTRING(REPLACE(salary_text, ',', '') FROM '[0-9]+\\.?[0-9]*[^0-9]+([0-9]+\\.?[0-9]*)'), '') AS NUMERIC),
                                    CAST(NULLIF(SUBSTRING(REPLACE(salary_text, ',', '') FROM '([0-9]+\\.?[0-9]*)'), '') AS NUMERIC)
                                )
                            ) / 2 
                            * CASE 
                                WHEN LOWER(salary_text) LIKE '%usd%' OR salary_text LIKE '%$%' THEN 25000
                                WHEN LOWER(salary_text) LIKE '%tr%' OR LOWER(salary_text) LIKE '%triệu%' THEN 1000000
                                ELSE 1 
                            END
                            / 
                            CASE 
                                WHEN LOWER(salary_text) LIKE '%năm%' OR LOWER(salary_text) LIKE '%year%' THEN 12
                                ELSE 1 
                            END
                        , 0)
                END AS BIGINT
            ) AS raw_salary_numeric,
            
            job_level, view_count, posted_date, expiry_date, 
            
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
                CAST(REPLACE(NULLIF(view_count, ''), '.0', '') AS INTEGER) AS view_count, 
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
                TRIM(REGEXP_REPLACE(job_description, '<[^>]*>', ' ', 'g')) AS job_description, 
                TRIM(REGEXP_REPLACE(job_requirements, '<[^>]*>', ' ', 'g')) AS job_requirements, 
                TRIM(REGEXP_REPLACE(job_benefits, '<[^>]*>', ' ', 'g')) AS job_benefits
            FROM staging.raw_dim_job_details WHERE job_id != ''
        ) cleaned_raw
    ) calculated_data
    ON CONFLICT (job_id) DO UPDATE SET 
        salary_numeric = EXCLUDED.salary_numeric,
        years_of_experience = EXCLUDED.years_of_experience;
    
    INSERT INTO dwh.dim_locations 
    SELECT DISTINCT CAST(REPLACE(NULLIF(job_id, ''), '.0', '') AS BIGINT), location_name 
    FROM staging.raw_dim_locations WHERE job_id != '' AND location_name != '' ON CONFLICT (job_id, location_name) DO NOTHING;
    
    INSERT INTO dwh.dim_skills 
    SELECT DISTINCT CAST(REPLACE(NULLIF(job_id, ''), '.0', '') AS BIGINT), skill_name 
    FROM staging.raw_dim_skills WHERE job_id != '' AND skill_name != '' ON CONFLICT (job_id, skill_name) DO NOTHING;

    INSERT INTO dwh.dim_categories 
    SELECT DISTINCT CAST(REPLACE(NULLIF(job_id, ''), '.0', '') AS BIGINT), category_name 
    FROM staging.raw_dim_categories WHERE job_id != '' AND category_name != '' ON CONFLICT (job_id, category_name) DO NOTHING;
    
    INSERT INTO dwh.fact_job_postings 
    SELECT CAST(REPLACE(NULLIF(job_id, ''), '.0', '') AS BIGINT), CAST(REPLACE(NULLIF(company_id, ''), '.0', '') AS BIGINT), CAST(REPLACE(NULLIF(industry_id, ''), '.0', '') AS BIGINT), CAST(NULLIF(crawled_at, '') AS TIMESTAMP) 
    FROM staging.raw_fact_job_postings WHERE job_id != '' AND company_id != '' AND industry_id != '' ON CONFLICT (job_id) DO NOTHING;
"""

SQL_VALIDATE_NULL = "SELECT COUNT(*) FROM dwh.fact_job_postings WHERE job_id IS NULL;"
SQL_VALIDATE_COUNT = "SELECT COUNT(*) FROM dwh.fact_job_postings;"
SQL_FETCH_JOBS_FOR_EMBEDDING = """
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
    LEFT JOIN (
        SELECT job_id, STRING_AGG(location_name, ', ') AS locations 
        FROM dwh.dim_locations GROUP BY job_id
    ) loc ON f.job_id = loc.job_id
    LEFT JOIN (
        SELECT job_id, STRING_AGG(skill_name, ', ') AS skills 
        FROM dwh.dim_skills GROUP BY job_id
    ) sk ON f.job_id = sk.job_id
    LEFT JOIN vector_dwh.dim_job_vectors v ON f.job_id = v.job_id
    WHERE v.job_id IS NULL;
"""
SQL_UPSERT_VECTOR = "INSERT INTO vector_dwh.dim_job_vectors (job_id, chunk_text, embedding) VALUES (:job_id, :chunk_text, :embedding) ON CONFLICT (job_id) DO UPDATE SET chunk_text = EXCLUDED.chunk_text, embedding = EXCLUDED.embedding;"