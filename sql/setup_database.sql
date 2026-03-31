-- ==============================================================================
-- KHỞI TẠO EXTENSION VÀ SCHEMA CƠ BẢN
-- ==============================================================================
CREATE EXTENSION IF NOT EXISTS vector;

CREATE SCHEMA IF NOT EXISTS audit;
CREATE TABLE IF NOT EXISTS audit.etl_log (
    log_id SERIAL PRIMARY KEY, 
    pipeline_name VARCHAR(100), 
    start_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP, 
    end_time TIMESTAMP, 
    status VARCHAR(50), 
    records_processed INTEGER DEFAULT 0, 
    error_message TEXT
);

-- ==============================================================================
-- TẦNG 1: STAGING (Dữ liệu thô từ Crawler đổ vào)
-- ==============================================================================
CREATE SCHEMA IF NOT EXISTS staging;

DROP TABLE IF EXISTS staging.raw_fact_job_postings, 
                     staging.raw_dim_job_details, 
                     staging.raw_dim_companies, 
                     staging.raw_dim_industries, 
                     staging.raw_dim_locations, 
                     staging.raw_dim_skills, 
                     staging.raw_dim_categories CASCADE;

CREATE TABLE staging.raw_fact_job_postings (job_id TEXT, company_id TEXT, industry_id TEXT, crawled_at TEXT);
CREATE TABLE staging.raw_dim_job_details (job_id TEXT, job_title TEXT, job_url TEXT, salary_text TEXT, job_level TEXT, view_count TEXT, posted_date TEXT, expiry_date TEXT, years_of_experience TEXT, job_description TEXT, job_requirements TEXT, job_benefits TEXT);
CREATE TABLE staging.raw_dim_companies (company_id TEXT, company_name TEXT, description TEXT, logo_url TEXT, profile_url TEXT);
CREATE TABLE staging.raw_dim_industries (industry_id TEXT, industry_name TEXT);
CREATE TABLE staging.raw_dim_locations (job_id TEXT, location_name TEXT);
CREATE TABLE staging.raw_dim_skills (job_id TEXT, skill_name TEXT);
CREATE TABLE staging.raw_dim_categories (job_id TEXT, category_name TEXT);

-- ==============================================================================
-- TẦNG 2: DATA WAREHOUSE (Dữ liệu đã chuẩn hóa, tối ưu cho Dashboard)
-- Mô hình: Snowflake Schema (Tối ưu lưu trữ đa trị - Multi-valued Dimensions)
-- ==============================================================================
CREATE SCHEMA IF NOT EXISTS dwh;

DROP TABLE IF EXISTS dwh.fact_job_postings, 
                     dwh.dim_locations, 
                     dwh.dim_skills, 
                     dwh.dim_categories, 
                     dwh.dim_job_details, 
                     dwh.dim_companies, 
                     dwh.dim_industries CASCADE;

-- 1. Các bảng Dimension độc lập
CREATE TABLE dwh.dim_companies (
    company_id BIGINT PRIMARY KEY, 
    company_name TEXT, 
    description TEXT, 
    logo_url TEXT, 
    profile_url TEXT
);

CREATE TABLE dwh.dim_industries (
    industry_id BIGINT PRIMARY KEY, 
    industry_name TEXT
);

-- 2. Bảng Dimension Chi tiết Công việc
CREATE TABLE dwh.dim_job_details (
    job_id BIGINT PRIMARY KEY, 
    job_title TEXT, 
    job_url TEXT, 
    salary_text TEXT, 
    salary_numeric BIGINT DEFAULT 0, 
    job_level TEXT, 
    view_count INTEGER, 
    posted_date DATE, 
    expiry_date DATE, 
    years_of_experience INTEGER DEFAULT 0, 
    job_description TEXT, 
    job_requirements TEXT, 
    job_benefits TEXT
);

-- 3. Các bảng Dimension Đa trị (Bridge Tables) - Nối với Job Details
CREATE TABLE dwh.dim_locations (
    job_id BIGINT, 
    location_name TEXT, 
    PRIMARY KEY (job_id, location_name), 
    FOREIGN KEY (job_id) REFERENCES dwh.dim_job_details(job_id) ON DELETE CASCADE
);

CREATE TABLE dwh.dim_skills (
    job_id BIGINT, 
    skill_name TEXT, 
    PRIMARY KEY (job_id, skill_name), 
    FOREIGN KEY (job_id) REFERENCES dwh.dim_job_details(job_id) ON DELETE CASCADE
);

CREATE TABLE dwh.dim_categories (
    job_id BIGINT, 
    category_name TEXT, 
    PRIMARY KEY (job_id, category_name), 
    FOREIGN KEY (job_id) REFERENCES dwh.dim_job_details(job_id) ON DELETE CASCADE
);

-- 4. Bảng FACT Trung tâm
CREATE TABLE dwh.fact_job_postings (
    job_id BIGINT PRIMARY KEY, 
    company_id BIGINT, 
    industry_id BIGINT, 
    crawled_at TIMESTAMP, 
    FOREIGN KEY (job_id) REFERENCES dwh.dim_job_details(job_id) ON DELETE CASCADE, 
    FOREIGN KEY (company_id) REFERENCES dwh.dim_companies(company_id) ON DELETE CASCADE, 
    FOREIGN KEY (industry_id) REFERENCES dwh.dim_industries(industry_id) ON DELETE CASCADE
);

-- ==============================================================================
-- TẦNG 3: VECTOR DATABASE (Dữ liệu cho AI RAG & Hybrid Search)
-- ==============================================================================
CREATE SCHEMA IF NOT EXISTS vector_dwh;
DROP TABLE IF EXISTS vector_dwh.dim_job_vectors CASCADE;

CREATE TABLE vector_dwh.dim_job_vectors (
    job_id BIGINT PRIMARY KEY, 
    chunk_text TEXT, 
    embedding VECTOR(384),
    FOREIGN KEY (job_id) REFERENCES dwh.dim_job_details(job_id) ON DELETE CASCADE
);