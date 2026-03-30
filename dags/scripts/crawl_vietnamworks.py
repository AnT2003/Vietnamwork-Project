# ==========================================
# FILE: crawl_vietnamworks.py (Chỉ lấy dữ liệu thô, BỎ hết logic làm sạch)
# ==========================================
import requests
import pandas as pd
import time
import os
import sys
from datetime import datetime
import urllib3

sys.stdout.reconfigure(encoding='utf-8')
urllib3.disable_warnings()

os.makedirs("data/daily", exist_ok=True)

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

class SequentialIDGenerator:
    def __init__(self): 
        self.counters = {}
        self.maps = {}
    def get_id(self, cat, val):
        if not val: return None
        if cat not in self.counters: 
            self.counters[cat] = 0
            self.maps[cat] = {}
        if val not in self.maps[cat]: 
            self.counters[cat] += 1
            self.maps[cat][val] = self.counters[cat]
        return self.maps[cat][val]

id_gen = SequentialIDGenerator()

def extract_job_from_json(j):
    jid = str(j.get("jobId", ""))
    if not jid: return None

    raw_url = j.get("jobUrl", "")
    alias = j.get("alias", "")
    if not raw_url and alias:
        raw_url = f"/{alias}-{jid}-jv"
    final_job_url = raw_url if raw_url.startswith("http") else f"{BASE_URL}/{raw_url.lstrip('/')}"

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

    struct_ben = []
    for b in (j.get("benefits") or []):
        name = b.get("benefitNameVI") or b.get("benefitName", "")
        val = b.get("benefitValue", "")
        if name: struct_ben.append(f"- {name}: {val}" if val else f"- {name}")

    skills = [s.get("skillName") for s in (j.get("skills") or []) if s.get("skillName")]
    locs = [l.get("cityNameVI") or l.get("address") for l in (j.get("workingLocations") or [])]
    
    app_raw = j.get("approvedOn") or ""
    exp_raw = j.get("expiredOn") or ""
    
    y_val = j.get("yearsOfExperience") or 0
    exp_final = f"{y_val} năm" if int(y_val) > 0 else "Không yêu cầu"
    
    # ==========================================
    # CẬP NHẬT LOGIC: LẤY TỪ jobFunction
    # ==========================================
    job_func = j.get("jobFunction") or {}
    
    # 1. Lấy Industry (Parent)
    industry_parent = job_func.get("parentNameVI") or job_func.get("parentName") or "Khác"
    
    # 2. Lấy Categories (Children)
    categories_list = []
    for child in job_func.get("children", []):
        cat_name = child.get("nameVI") or child.get("name")
        if cat_name:
            categories_list.append(cat_name)

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
        "views": 0,
        "skills": skills,
        "provinces": list(set([loc for loc in locs if loc]))
    }

def start_crawl(target_total=100, output_dir="data/daily"):
    print(f"[*] BẮT ĐẦU CÀO DỮ LIỆU (Mục tiêu: {target_total} jobs, Lưu tại: {output_dir})")
    
    fact_postings, dim_details, dim_companies, dim_industries = [], [], [], []
    dim_locations, dim_skills, dim_categories = [], [], [] 
    
    current_page = 0
    total_scraped = 0
    
    while total_scraped < target_total:
        try:
            payload = {
                "userId": 7574593,
                "query": "",
                "filter": [],
                "ranges": [],
                "order": [{"field": "approvedOn", "value": "desc"}],
                "hitsPerPage": 50,
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
            
            res = requests.post(API_SEARCH_URL, headers=HEADERS, json=payload, timeout=10, verify=False)
            api_jobs = res.json().get('data', [])
            if not api_jobs: break
        except Exception as e: 
            print(f"Lỗi gọi API: {e}")
            break

        for j in api_jobs:
            if total_scraped >= target_total: break
            
            print(f"   🔄 [{total_scraped+1}/{target_total}] Đang cào Job ID: {j.get('jobId', 'Unknown')} ...")
            
            d = extract_job_from_json(j)
            
            if d:
                jid = d['job_id']
                cid = id_gen.get_id("company", d['company'])
                iid = id_gen.get_id("industry", d['industry'])
                
                fact_postings.append({"job_id": jid, "company_id": cid, "industry_id": iid, "crawled_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S")})
                dim_details.append({"job_id": jid, "job_title": d['title'], "job_url": d['job_url'], "salary_text": d['salary'], "job_level": d['level'], "view_count": 0, "posted_date": d['posted_date'], "expiry_date": d['expired_date'], "years_of_experience": d['exp'], "job_description": d['description'], "job_requirements": d['requirements'], "job_benefits": d['benefits']})
                
                if not any(c['company_id'] == cid for c in dim_companies):
                    dim_companies.append({"company_id": cid, "company_name": d['company'], "description": d['comp_desc'], "logo_url": d['logo'], "profile_url": d['profile_url']})
                
                if iid and not any(i['industry_id'] == iid for i in dim_industries):
                    dim_industries.append({"industry_id": iid, "industry_name": d['industry']})
                
                for prov in d['provinces']: dim_locations.append({"job_id": jid, "location_name": prov})
                for sk in d['skills']: dim_skills.append({"job_id": jid, "skill_name": sk})
                for cat in d['categories']: dim_categories.append({"job_id": jid, "category_name": cat})
                
                total_scraped += 1
                
        current_page += 1

    pd.DataFrame(fact_postings).to_csv(os.path.join(output_dir, "fact_job_postings.csv"), index=False, encoding="utf-8-sig")
    pd.DataFrame(dim_details).to_csv(os.path.join(output_dir, "dim_job_details.csv"), index=False, encoding="utf-8-sig")
    pd.DataFrame(dim_companies).to_csv(os.path.join(output_dir, "dim_companies.csv"), index=False, encoding="utf-8-sig")
    pd.DataFrame(dim_industries).to_csv(os.path.join(output_dir, "dim_industries.csv"), index=False, encoding="utf-8-sig")
    pd.DataFrame(dim_locations).to_csv(os.path.join(output_dir, "dim_locations.csv"), index=False, encoding="utf-8-sig")
    pd.DataFrame(dim_skills).to_csv(os.path.join(output_dir, "dim_skills.csv"), index=False, encoding="utf-8-sig")
    pd.DataFrame(dim_categories).to_csv(os.path.join(output_dir, "dim_categories.csv"), index=False, encoding="utf-8-sig")
    
    print(f"[*] HOÀN TẤT! Đã lưu {total_scraped} jobs vào thư mục {output_dir}.")
    