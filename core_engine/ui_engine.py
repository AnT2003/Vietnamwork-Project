import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from core_engine.ai_engine import extract_text_from_pdf, fetch_and_rank_jobs, generate_llm_response
from core_engine.dashboard_engine import get_filter_options, load_dashboard_data

# ==========================================
# 1. CSS & HEADER
# ==========================================
def load_css_and_header():
    st.markdown("""
    <link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.4.0/css/all.min.css">
    <style>
        @import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700;800;900&display=swap');
        html, body, [class*="css"] { font-family: 'Inter', sans-serif !important; background-color: #f8fafc; }
        .job-card { background: #ffffff; border: 1px solid #e2e8f0; border-left: 5px solid #2563eb; border-radius: 12px; padding: 20px; margin: 15px 0; box-shadow: 0 4px 6px -1px rgba(0,0,0,0.1); }
        .job-card h3 { margin: 0 0 10px 0; color: #1e40af; font-size: 1.2rem; font-weight: 800; }
        .job-card h3 a { color: #2563eb; text-decoration: none; }
        .job-card p { margin: 5px 0; color: #334155; font-size: 0.95rem; }
        .kpi-card { background: white; border-radius: 12px; padding: 20px; border: 1px solid #eef0f2; box-shadow: 0 4px 15px rgba(0,0,0,0.03); }
        .kpi-value { font-size: 2rem; font-weight: 900; color: #1e293b; }
        [data-testid="stSidebar"] { background-color: #f1f5f9; }
    </style>
    """, unsafe_allow_html=True)
    
    st.markdown("""
    <div style="display: flex; align-items: center; gap: 12px; margin-bottom: 25px;">
        <div style="width: 40px; height: 40px; background: #2563eb; border-radius: 10px; display: flex; align-items: center; justify-content: center; color: white;">
            <i class="fas fa-brain" style="font-size: 1.4rem;"></i>
        </div>
        <h1 style="margin: 0; font-size: 1.5rem; font-weight: 800; color: #1e293b;">VietnamWorks AI Analytics</h1>
    </div>
    """, unsafe_allow_html=True)

def render_dash_kpi(title, value, icon, color):
    st.markdown(f"""
    <div class="kpi-card" style="border-top: 4px solid {color};">
        <div style="color: #64748b; font-size: 0.8rem; font-weight: 700; text-transform: uppercase;">{title}</div>
        <div style="display: flex; justify-content: space-between; align-items: center; margin-top: 10px;">
            <div class="kpi-value">{value}</div>
            <div style="color: {color}; font-size: 1.5rem;"><i class="{icon}"></i></div>
        </div>
    </div>
    """, unsafe_allow_html=True)

# ==========================================
# 2. TABS RENDERING
# ==========================================
def render_overview_tab():
    st.markdown("<br>", unsafe_allow_html=True)
    col1, col2 = st.columns([1.2, 1])
    with col1:
        st.markdown("""
        <h1 style="font-size: 3.5rem; font-weight: 900; line-height: 1.1; color: #1e293b;">Smart <span style="color: #2563eb;">Recruitment</span><br>Insights Engine</h1>
        <p style="font-size: 1.1rem; color: #64748b; margin: 25px 0;">Hệ thống phân tích dữ liệu tuyển dụng chuyên sâu kết hợp Hybrid Search và RAG.</p>
        """, unsafe_allow_html=True)
    with col2:
        st.image("https://img.freepik.com/free-vector/data-analysis-concept-illustration_114360-8013.jpg")

def render_dashboard_tab():
    try:
        list_ind, list_cat, list_lvl = get_filter_options()
        with st.sidebar:
            st.header("🔍 Filters")
            sel_ind = st.selectbox("Industry", list_ind)
            sel_cat = st.selectbox("Profession", list_cat)
            sel_lvl = st.selectbox("Level", list_lvl)
        
        # Gọi Database (Lấy 7 biến)
        m, s, se, tm, c, l, cs = load_dashboard_data(sel_ind, sel_cat, sel_lvl)
        
        total_jobs = f"{int(m['total_jobs']):,}" if pd.notnull(m['total_jobs']) else "0"
        total_companies = f"{int(m['total_companies']):,}" if pd.notnull(m['total_companies']) else "0"
        avg_salary = f"{float(m['avg_salary'])/1000000:.1f} M" if pd.notnull(m['avg_salary']) else "N/A"
        top_skill = str(s.iloc[0]['skill_name']) if not s.empty else "N/A"
        
        c1, c2, c3, c4 = st.columns(4)
        with c1: render_dash_kpi("Tổng Jobs", total_jobs, "fas fa-briefcase", "#3b82f6")
        with c2: render_dash_kpi("Công ty", total_companies, "fas fa-building", "#f59e0b")
        with c3: render_dash_kpi("Lương TB", avg_salary, "fas fa-money-bill-wave", "#10b981")
        with c4: render_dash_kpi("Kỹ năng Hot", top_skill, "fas fa-star", "#8b5cf6")

        st.markdown("<br>", unsafe_allow_html=True)
        
        # --- HÀNG 1: SKILLS & LEVEL ---
        col1, col2 = st.columns([1.2, 1])
        with col1:
            st.subheader("📊 Top Kỹ Năng Tuyển Dụng")
            if not s.empty:
                fig1 = px.bar(s.sort_values(by="job_count", ascending=True), x='job_count', y='skill_name', orientation='h', color_discrete_sequence=['#3b82f6'])
                fig1.update_layout(margin=dict(l=0, r=0, t=0, b=0))
                st.plotly_chart(fig1, use_container_width=True)
            else: st.info("Chưa có dữ liệu")

        with col2:
            st.subheader("📈 Phân Bổ Cấp Bậc")
            if not l.empty:
                fig2 = px.pie(l, values='job_count', names='job_level', hole=0.4, color_discrete_sequence=px.colors.sequential.Blues_r)
                fig2.update_layout(margin=dict(l=0, r=0, t=0, b=0))
                st.plotly_chart(fig2, use_container_width=True)
            else: st.info("Chưa có dữ liệu")

        st.markdown("<br>", unsafe_allow_html=True)
        
        # --- HÀNG 2: COMPANIES & SALARIES ---
        col3, col4 = st.columns([1, 1.2])
        with col3:
            st.subheader("🏢 Top Công Ty Tuyển Dụng")
            if not c.empty:
                fig3 = px.bar(c.sort_values(by="job_count", ascending=True), x='job_count', y='company_name', orientation='h', color_discrete_sequence=['#f59e0b'])
                fig3.update_layout(margin=dict(l=0, r=0, t=0, b=0))
                st.plotly_chart(fig3, use_container_width=True)
            else: st.info("Chưa có dữ liệu")

        with col4:
            st.subheader("💰 Mức Lương Trung Bình Theo Ngành")
            if not cs.empty:
                fig4 = px.bar(cs.sort_values(by="avg_salary", ascending=True), x='avg_salary', y='category_name', orientation='h', color_discrete_sequence=['#10b981'])
                fig4.update_layout(margin=dict(l=0, r=0, t=0, b=0))
                st.plotly_chart(fig4, use_container_width=True)
            else: st.info("Chưa có dữ liệu")

        st.markdown("<br>", unsafe_allow_html=True)
        
        # --- HÀNG 3: TREEMAP & EXPERIENCE ---
        col5, col6 = st.columns([1, 1])
        with col5:
            st.subheader("🗺️ Phân Bổ Job Theo Lĩnh Vực")
            if not tm.empty:
                fig5 = px.treemap(tm, path=['industry_name', 'category_name'], values='job_count', color='job_count', color_continuous_scale='Blues')
                fig5.update_layout(margin=dict(l=0, r=0, t=0, b=0))
                st.plotly_chart(fig5, use_container_width=True)
            else: st.info("Chưa có dữ liệu")

        with col6:
            st.subheader("📈 Lương & Nhu Cầu Theo Kinh Nghiệm")
            if not se.empty:
                fig6 = go.Figure()
                fig6.add_trace(go.Bar(x=se['years_of_experience'], y=se['job_count'], name='Số lượng Job', marker_color='#cbd5e1', yaxis='y1'))
                fig6.add_trace(go.Scatter(x=se['years_of_experience'], y=se['avg_salary'], name='Lương TB', mode='lines+markers', marker_color='#3b82f6', yaxis='y2'))
                fig6.update_layout(yaxis=dict(title='Số lượng Job', side='left'), yaxis2=dict(title='Lương TB', side='right', overlaying='y', showgrid=False), margin=dict(l=0, r=0, t=0, b=0), legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1))
                st.plotly_chart(fig6, use_container_width=True)
            else: st.info("Chưa có dữ liệu")
            
    except Exception as e:
        st.error(f"⚠️ Đã xảy ra lỗi khi tải Dashboard: {e}")

def render_prediction_tab():
    if "messages" not in st.session_state: st.session_state.messages = []
    if "cv_text" not in st.session_state: st.session_state.cv_text = ""

    with st.sidebar:
        st.header("📄 CV Analysis")
        uploaded_file = st.file_uploader("Upload your CV (PDF)", type="pdf")
        if uploaded_file:
            with st.spinner("Extracting CV..."):
                st.session_state.cv_text = extract_text_from_pdf(uploaded_file)
                st.success("✅ CV Parsed!")
        
        if st.button("🗑️ Clear Chat History"):
            st.session_state.messages = []
            st.rerun()

    chat_container = st.container(height=500)
    with chat_container:
        for m in st.session_state.messages:
            with st.chat_message(m["role"], avatar="🧠" if m["role"]=="assistant" else "👤"):
                st.markdown(m["content"], unsafe_allow_html=True)

    if prompt := st.chat_input("VD: Tìm việc Data Engineer tại Hà Nội..."):
        st.session_state.messages.append({"role": "user", "content": prompt})
        with chat_container:
            st.chat_message("user", avatar="👤").markdown(prompt)
            with st.chat_message("assistant", avatar="🧠"):
                with st.spinner("Analyzing Database..."):
                    jobs_df, is_fallback, req_loc = fetch_and_rank_jobs(prompt, st.session_state.cv_text, top_k=5)
                    reply = generate_llm_response(prompt, jobs_df, st.session_state.cv_text, st.session_state.messages[:-1], is_fallback, req_loc)
                    formatted_reply = reply.replace("\n", "<br>")
                    st.markdown(formatted_reply, unsafe_allow_html=True)
                    st.session_state.messages.append({"role": "assistant", "content": formatted_reply})