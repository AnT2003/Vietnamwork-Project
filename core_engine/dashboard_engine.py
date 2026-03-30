import pandas as pd
from sqlalchemy import text
from core_engine.ai_engine import engine

def get_filter_options():
    with engine.connect() as conn:
        res_ind = conn.execute(text("SELECT DISTINCT industry_name FROM dwh.dim_industries WHERE industry_name IS NOT NULL")).fetchall()
        res_cat = conn.execute(text("SELECT DISTINCT category_name FROM dwh.dim_categories WHERE category_name IS NOT NULL")).fetchall()
        res_lvl = conn.execute(text("SELECT DISTINCT job_level FROM dwh.dim_job_details WHERE job_level IS NOT NULL")).fetchall()
    return ["All"] + [r[0] for r in res_ind], ["All"] + [r[0] for r in res_cat], ["All"] + [r[0] for r in res_lvl]

def load_dashboard_data_json(ind_f, cat_f, lvl_f):
    wheres, p = ["1=1"], {}
    if ind_f != "All": wheres.append("i.industry_name = :ind"); p['ind'] = ind_f
    if cat_f != "All": wheres.append("cat.category_name = :cat"); p['cat'] = cat_f
    if lvl_f != "All": wheres.append("d.job_level = :lvl"); p['lvl'] = lvl_f
    ws, bj = " AND ".join(wheres), "FROM dwh.fact_job_postings f JOIN dwh.dim_job_details d ON f.job_id = d.job_id LEFT JOIN dwh.dim_industries i ON f.industry_id = i.industry_id LEFT JOIN dwh.dim_categories cat ON f.job_id = cat.job_id"
    
    with engine.connect() as conn:
        # KPI Metrics
        m = conn.execute(text(f"SELECT COUNT(DISTINCT f.job_id) as tj, COUNT(DISTINCT f.company_id) as tc, AVG(NULLIF(d.salary_numeric, 0)) as avgs {bj} WHERE {ws}"), p).fetchone()
        
        # SQL Queries
        s = pd.DataFrame(conn.execute(text(f"SELECT s.skill_name, COUNT(DISTINCT f.job_id) as count {bj} JOIN dwh.dim_skills s ON f.job_id = s.job_id WHERE {ws} GROUP BY s.skill_name ORDER BY count DESC LIMIT 10"), p).fetchall(), columns=['skill_name', 'count'])
        l = pd.DataFrame(conn.execute(text(f"SELECT d.job_level, COUNT(DISTINCT f.job_id) as count {bj} WHERE {ws} GROUP BY d.job_level ORDER BY count DESC"), p).fetchall(), columns=['job_level', 'count'])
        c = pd.DataFrame(conn.execute(text(f"SELECT c.company_name, COUNT(DISTINCT f.job_id) as count {bj} JOIN dwh.dim_companies c ON f.company_id = c.company_id WHERE {ws} GROUP BY c.company_name ORDER BY count DESC LIMIT 10"), p).fetchall(), columns=['company_name', 'count'])
        cs = pd.DataFrame(conn.execute(text(f"SELECT cat.category_name, AVG(NULLIF(d.salary_numeric, 0)) as avg_salary {bj} WHERE {ws} AND d.salary_numeric > 0 AND cat.category_name IS NOT NULL GROUP BY cat.category_name ORDER BY avg_salary DESC LIMIT 10"), p).fetchall(), columns=['category_name', 'avg_salary'])
        se = pd.DataFrame(conn.execute(text(f"SELECT d.years_of_experience, AVG(NULLIF(d.salary_numeric, 0)) as avg_salary, COUNT(DISTINCT f.job_id) as count {bj} WHERE {ws} AND d.salary_numeric > 0 AND d.years_of_experience <= 10 GROUP BY d.years_of_experience ORDER BY d.years_of_experience"), p).fetchall(), columns=['years_of_experience', 'avg_salary', 'count'])
        tm = pd.DataFrame(conn.execute(text(f"SELECT i.industry_name, cat.category_name, COUNT(DISTINCT f.job_id) as count {bj} WHERE {ws} AND i.industry_name IS NOT NULL AND cat.category_name IS NOT NULL GROUP BY i.industry_name, cat.category_name"), p).fetchall(), columns=['industry_name', 'category_name', 'count'])
        
        # 🟢 XỬ LÝ TREEMAP: ĐÃ ĐỔI TÊN BIẾN CHUẨN THÀNH 'industry' VÀ 'category'
        tree_data = []
        if not tm.empty: 
            # 1. Lọc Top 5 Lĩnh vực có tổng Job cao nhất
            top_ind = tm.groupby('industry_name')['count'].sum().nlargest(5).index
            tm = tm[tm['industry_name'].isin(top_ind)]
            
            # 2. Sort giảm dần để xếp hạng
            tm = tm.sort_values(['industry_name', 'count'], ascending=[True, False])
            
            # 3. Chỉ lấy Top 3 ngành mỗi lĩnh vực và đánh rank (0, 1, 2)
            tm['rank'] = tm.groupby('industry_name').cumcount()
            tm = tm[tm['rank'] < 3]
            
            for _, row in tm.iterrows():
                tree_data.append({
                    "industry": str(row['industry_name']),  # SỬA: Đổi từ 'category' thành 'industry' (Khối Cha)
                    "category": str(row['category_name']),  # SỬA: Đổi từ 'type' thành 'category' (Khối Con)
                    "value": int(row['count']),
                    "rank": int(row['rank']) # 0 là cao nhất (Đậm nhất), 2 là nhạt nhất
                })

        return {
            "kpi": {
                "total_jobs": f"{int(m[0]):,}" if m[0] else "0",
                "total_companies": f"{int(m[1]):,}" if m[1] else "0",
                "avg_salary": f"{float(m[2])/1000000:.1f} M" if m[2] else "N/A",
                "top_skill": str(s.iloc[0]['skill_name']) if not s.empty else "N/A"
            },
            "bar_skills": {"labels": s['skill_name'].astype(str).tolist() if not s.empty else [], "data": [int(x) for x in s['count'].tolist()] if not s.empty else []},
            "pie_levels": {"labels": l['job_level'].astype(str).tolist() if not l.empty else [], "data": [int(x) for x in l['count'].tolist()] if not l.empty else []},
            "bar_companies": {"labels": c['company_name'].astype(str).tolist() if not c.empty else [], "data": [int(x) for x in c['count'].tolist()] if not c.empty else []},
            "bar_salaries": {"labels": cs['category_name'].astype(str).tolist() if not cs.empty else [], "data": [round(float(x)/1000000, 1) for x in cs['avg_salary'].tolist()] if not cs.empty else []},
            "mix_exp": {
                "labels": se['years_of_experience'].astype(str).tolist() if not se.empty else [], 
                "jobs": [int(x) for x in se['count'].tolist()] if not se.empty else [],
                "salary": [round(float(x)/1000000, 1) if x else 0.0 for x in se['avg_salary'].tolist()] if not se.empty else []
            },
            "treemap": tree_data
        }