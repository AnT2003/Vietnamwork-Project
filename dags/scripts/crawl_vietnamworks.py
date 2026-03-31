# ==========================================
# FILE: crawl_vietnamworks.py (Crawler siêu tốc - Chống trùng lặp dữ liệu)
# Nhiệm vụ: Tự động thu thập dữ liệu việc làm từ API, lọc bỏ dữ liệu cũ 
# và bóc tách thành các bảng Fact/Dimension để chuẩn bị cho quá trình ETL.
# ==========================================
import requests
import pandas as pd
import time
import os
import sys
from datetime import datetime
import urllib3

# Cấu hình hệ thống cơ bản: Chống lỗi font chữ và tắt cảnh báo bảo mật SSL
sys.stdout.reconfigure(encoding='utf-8')
urllib3.disable_warnings()

# Tạo sẵn thư mục lưu trữ dữ liệu thô (Staging Area) nếu chưa tồn tại
os.makedirs("data/daily", exist_ok=True)

# Cấu hình API Endpoint và HTTP Headers giả lập trình duyệt để tránh bị chặn (Anti-bot)
API_SEARCH_URL = "https://ms.vietnamworks.com/job-search/v1.0/search"
BASE_URL = "https://www.vietnamworks.com"

HEADERS = {
    "Accept": "*/*",
    "Accept-Language": "vi",
    "Connection": "keep-alive",
    "Content-Type": "application/json",
    "Origin": "https://www.vietnamworks.com",
    "Referer": "https://www.vietnamworks.com/",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/146.0.0.0 Safari/537.36",
    "X-Source": "Page-Container"
}

# ==============================================================================
# HÀM TẠO KHÓA CHÍNH TỰ TĂNG (Surrogate Key Generator)
# Giải quyết vấn đề dữ liệu API không cung cấp sẵn ID chuẩn cho Công ty/Ngành nghề
# ==============================================================================
class SequentialIDGenerator:
    def __init__(self): 
        self.counters = {} # Bộ đếm ID cho từng danh mục
        self.maps = {}     # Bộ nhớ ánh xạ: Tên -> ID
        
    def get_id(self, cat, val):
        if not val: return None
        if cat not in self.counters: 
            self.counters[cat] = 0
            self.maps[cat] = {}
        # Nếu giá trị chưa tồn tại, cấp phát 1 ID mới và lưu lại
        if val not in self.maps[cat]: 
            self.counters[cat] += 1
            self.maps[cat][val] = self.counters[cat]
        return self.maps[cat][val]

id_gen = SequentialIDGenerator()

# ==============================================================================
# HÀM TIỀN XỬ LÝ (Pre-processing): Bóc tách JSON thô thành Dictionary chuẩn
# ==============================================================================
def extract_job_from_json(j):
    jid = str(j.get("jobId", ""))
    if not jid: return None

    # Xử lý URL công việc: Ghép URL đầy đủ nếu API chỉ trả về alias
    raw_url = j.get("jobUrl", "")
    alias = j.get("alias", "")
    if not raw_url and alias:
        raw_url = f"/{alias}-{jid}-jv"
    final_job_url = raw_url if raw_url.startswith("http") else f"{BASE_URL}/{raw_url.lstrip('/')}"

    # Logic làm sạch mức lương: Ưu tiên lương đã format, xử lý trường hợp lương ẩn/thỏa thuận
    sal_raw = str(j.get("salary") or "")
    sal_pretty_vi = str(j.get("prettySalaryVI") or "")
    sal_pretty = str(j.get("prettySalary") or "")
    
    sal_text = sal_pretty_vi or sal_pretty or sal_raw
    sal_min = j.get("salaryMin")
    sal_max = j.get("salaryMax")
    is_visible = j.get("isSalaryVisible", True)

    if not sal_text or "thương lượng" in sal_text.lower() or "negotiable" in sal_text.lower():
        if sal_min and sal_max: sal_text = f"{sal_min} - {sal_max}"
        elif sal_min: sal_text = f"Từ {sal_min}"
        elif sal_max: sal_text = f"Đến {sal_max}"
        else: sal_text = "Thương lượng"
            
    if not is_visible and sal_text != "Thương lượng": sal_text = f"{sal_text} (Ẩn)"
    elif not is_visible: sal_text = "Thương lượng"

    # Trích xuất và định dạng danh sách phúc lợi (Benefits) thành chuỗi có gạch đầu dòng
    struct_ben = []
    for b in (j.get("benefits") or []):
        name = b.get("benefitNameVI") or b.get("benefitName", "")
        val = b.get("benefitValue", "")
        if name: struct_ben.append(f"- {name}: {val}" if val else f"- {name}")

    # Trích xuất dữ liệu đa trị (Multi-valued): Kỹ năng, Địa điểm, Danh mục
    skills = [s.get("skillName") for s in (j.get("skills") or []) if s.get("skillName")]
    locs = [l.get("cityNameVI") or l.get("address") for l in (j.get("workingLocations") or [])]
    
    app_raw = j.get("approvedOn") or ""
    exp_raw = j.get("expiredOn") or ""
    
    y_val = j.get("yearsOfExperience") or 0
    exp_final = f"{y_val} năm" if int(y_val) > 0 else "Không yêu cầu"
    
    job_func = j.get("jobFunction") or {}
    industry_parent = job_func.get("parentNameVI") or job_func.get("parentName") or "Khác"
    
    categories_list = []
    for child in job_func.get("children", []):
        cat_name = child.get("nameVI") or child.get("name")
        if cat_name:
            categories_list.append(cat_name)

    # Đóng gói và trả về một bộ dữ liệu hoàn chỉnh, sẵn sàng nạp vào Data Warehouse
    return {
        "job_id": jid,
        "title": j.get("jobTitle", ""),
        "company": j.get("companyName", ""),
        "salary": sal_text,
        "level": j.get("jobLevelVI", ""),
        "description": j.get("jobDescription", ""),
        "requirements": j.get("jobRequirement", ""),
        "benefits": "\n".join(struct_ben),
        "posted_date": app_raw.split("T")[0] if app_raw else "",
        "expired_date": exp_raw.split("T")[0] if exp_raw else "",
        "industry": industry_parent,
        "categories": categories_list,
        "exp": exp_final,
        "comp_desc": j.get("companyProfile", ""),
        "logo": j.get("companyLogo", ""),
        "job_url": final_job_url,
        "profile_url": f"{BASE_URL}/nha-tuyen-dung/c{j.get('companyId')}" if j.get("companyId") else "",
        "skills": skills,
        "provinces": list(set([loc for loc in locs if loc]))
    }

# ==============================================================================
# LUỒNG CHẠY CHÍNH CỦA CRAWLER (Quản lý Phân trang, Network & Tách bảng)
# ==============================================================================
def start_crawl(target_total=100, output_dir="data/daily", existing_job_ids=None):
    # 1. TRUY VẤN DATABASE LẤY ID CŨ (DELTA LOAD)
    # Kỹ thuật đọc trước ID giúp Crawler tự động lướt qua các Job đã từng cào
    if existing_job_ids is None:
        existing_job_ids = set()
        try:
            from sqlalchemy import create_engine, text
            from config import DB_URI
            engine = create_engine(DB_URI)
            with engine.connect() as conn:
                res = conn.execute(text("SELECT CAST(job_id AS VARCHAR) FROM dwh.dim_job_details"))
                existing_job_ids = set(row[0] for row in res.fetchall())
            print(f"[*] Đã nạp {len(existing_job_ids)} Job ID từ Database để đối chiếu trùng lặp.")
        except Exception:
            print("[*] Không tìm thấy Database (Bỏ qua check trùng, cào từ đầu).")

    print(f"[*] BẮT ĐẦU LÙNG SỤC (Mục tiêu: Tìm đúng {target_total} jobs MỚI, Lưu tại: {output_dir})")
    
    # Khởi tạo các mảng chứa dữ liệu đã được tách theo mô hình Snowflake Schema
    fact_postings, dim_details, dim_companies, dim_industries = [], [], [], []
    dim_locations, dim_skills, dim_categories = [], [], [] 
    
    current_page = 0
    total_new_scraped = 0
    consecutive_empty_pages = 0
    
    # 2. TỐI ƯU TỐC ĐỘ: Dùng Session Management để giữ nguyên kết nối TCP/IP
    # Giúp tăng tốc độ request lên đáng kể so với việc gọi requests.post() độc lập
    session = requests.Session()
    session.headers.update(HEADERS)
    
    while total_new_scraped < target_total:
        try:
            # Xây dựng Payload truy vấn API (Sử dụng HitsPerPage=100 để giảm số vòng lặp tải trang)
            payload = {
                "userId": 0, # Xóa cứng ID user để tránh rủi ro lỗi phân quyền
                "query": "",
                "filter": [],
                "ranges": [],
                "order": [{"field": "approvedOn", "value": "desc"}],
                "hitsPerPage": 100, 
                "page": current_page,
                "retrieveFields": [
                    "address", "benefits", "jobTitle", "salaryMax", "isSalaryVisible", 
                    "jobLevelVI", "isShowLogo", "salaryMin", "companyLogo", "userId", 
                    "jobLevel", "jobLevelId", "jobId", "jobUrl", "companyId", "approvedOn", 
                    "isAnonymous", "alias", "expiredOn", "jobFunction", 
                    "workingLocations", "services", "companyName", "salary", "onlineOn", 
                    "simpleServices", "visibilityDisplay", "isShowLogoInSearch", "priorityOrder", 
                    "skills", "profilePublishedSiteMask", "jobDescription", "jobRequirement", 
                    "prettySalary", "requiredCoverLetter", "languageSelectedVI", "languageSelected", 
                    "languageSelectedId", "typeWorkingId", "createdOn", "isAdrLiteJob",
                    "yearsOfExperience", "companyProfile"
                ],
                "summaryVersion": ""
            }
            
            res = session.post(API_SEARCH_URL, json=payload, timeout=15, verify=False)
            api_jobs = res.json().get('data', [])
            
            # 3. LOGIC THOÁT KHỎI VÒNG LẶP (Break Condition):
            # Nếu lật 2 trang liên tiếp mà API trả về mảng rỗng -> Đã vét cạn dữ liệu máy chủ
            if not api_jobs: 
                consecutive_empty_pages += 1
                if consecutive_empty_pages >= 2:
                    print("\n[!] CẢNH BÁO: Đã quét đến ranh giới cuối cùng của VietnamWorks.")
                    print(f"[!] Dừng lại. Tổng cộng chỉ thu thập được {total_new_scraped} job mới.")
                    break
                current_page += 1
                continue
                
            consecutive_empty_pages = 0
            
        except Exception as e: 
            # Cơ chế Retry khi rớt mạng
            print(f"⚠️ Lỗi mạng ở trang {current_page}: {e}. Đang thử lại...")
            time.sleep(2)
            continue

        skipped_in_page = 0
        
        for j in api_jobs:
            if total_new_scraped >= target_total: break
            
            jid = str(j.get("jobId", ""))
            
            # KIỂM TRA TRÙNG LẶP CỰC NHANH (Smart Skip)
            # Bỏ qua ngay lập tức các Job ID đã nằm trong Database
            if not jid or jid in existing_job_ids:
                skipped_in_page += 1
                continue 

            d = extract_job_from_json(j)
            
            if d:
                # Cập nhật ID mới vào Memory Set để tránh trùng lặp chính nó trong cùng phiên cào
                existing_job_ids.add(d['job_id']) 
                
                # Cấp phát Surrogate Key cho Dimension
                cid = id_gen.get_id("company", d['company'])
                iid = id_gen.get_id("industry", d['industry'])
                
                # Phân rã dữ liệu (Data Normalization) vào các bảng tương ứng
                fact_postings.append({"job_id": d['job_id'], "company_id": cid, "industry_id": iid, "crawled_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S")})
                dim_details.append({"job_id": d['job_id'], "job_title": d['title'], "job_url": d['job_url'], "salary_text": d['salary'], "job_level": d['level'], "posted_date": d['posted_date'], "expiry_date": d['expired_date'], "years_of_experience": d['exp'], "job_description": d['description'], "job_requirements": d['requirements'], "job_benefits": d['benefits']})
                
                if not any(c['company_id'] == cid for c in dim_companies):
                    dim_companies.append({"company_id": cid, "company_name": d['company'], "description": d['comp_desc'], "logo_url": d['logo'], "profile_url": d['profile_url']})
                
                if iid and not any(i['industry_id'] == iid for i in dim_industries):
                    dim_industries.append({"industry_id": iid, "industry_name": d['industry']})
                
                # Xử lý các Bridge Tables (Bảng nhánh đa trị)
                for prov in d['provinces']: dim_locations.append({"job_id": d['job_id'], "location_name": prov})
                for sk in d['skills']: dim_skills.append({"job_id": d['job_id'], "skill_name": sk})
                for cat in d['categories']: dim_categories.append({"job_id": d['job_id'], "category_name": cat})
                
                total_new_scraped += 1
                if total_new_scraped % 10 == 0 or total_new_scraped == target_total:
                    print(f"   ✅ Đã thu thập {total_new_scraped}/{target_total} Job MỚI...")
        
        # Báo cáo kết quả bỏ qua Job cũ của từng trang
        if skipped_in_page > 0:
            print(f"   ⏩ Trang {current_page}: Tự động lướt qua {skipped_in_page} Jobs đã trùng lặp.")
            
        current_page += 1

    # ==============================================================================
    # XUẤT DỮ LIỆU (EXPORT): Đóng gói dữ liệu ra các file CSV
    # ==============================================================================
    pd.DataFrame(fact_postings).to_csv(os.path.join(output_dir, "fact_job_postings.csv"), index=False, encoding="utf-8-sig")
    pd.DataFrame(dim_details).to_csv(os.path.join(output_dir, "dim_job_details.csv"), index=False, encoding="utf-8-sig")
    pd.DataFrame(dim_companies).to_csv(os.path.join(output_dir, "dim_companies.csv"), index=False, encoding="utf-8-sig")
    pd.DataFrame(dim_industries).to_csv(os.path.join(output_dir, "dim_industries.csv"), index=False, encoding="utf-8-sig")
    pd.DataFrame(dim_locations).to_csv(os.path.join(output_dir, "dim_locations.csv"), index=False, encoding="utf-8-sig")
    pd.DataFrame(dim_skills).to_csv(os.path.join(output_dir, "dim_skills.csv"), index=False, encoding="utf-8-sig")
    pd.DataFrame(dim_categories).to_csv(os.path.join(output_dir, "dim_categories.csv"), index=False, encoding="utf-8-sig")
    
    print(f"[*] HOÀN TẤT! Đã đóng gói thành công {total_new_scraped} jobs mới tinh vào thư mục {output_dir}.")