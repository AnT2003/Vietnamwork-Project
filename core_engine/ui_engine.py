import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from core_engine.ai_engine import extract_text_from_pdf, fetch_and_rank_jobs, generate_llm_response
from core_engine.dashboard_engine import get_filter_options, load_dashboard_data

TAILWIND_COLORS = ['#3b82f6', '#10b981', '#f59e0b', '#f43f5e', '#8b5cf6', '#06b6d4', '#84cc16']

def load_css_and_header():
    st.markdown("""
    <link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.4.0/css/all.min.css">
    <style>
        @import url('https://fonts.googleapis.com/css2?family=Plus+Jakarta+Sans:wght@300;400;500;600;700;800;900&display=swap');
        html, body, [class*="css"] { font-family: 'Plus Jakarta Sans', sans-serif !important; }
        .stApp { background-color: #f7f9fc; } 
        header {visibility: hidden;} #MainMenu {visibility: hidden;} footer {visibility: hidden;}
        
        .stTabs [data-baseweb="tab-list"] { gap: 24px; background: transparent; padding: 15px 40px 0px 40px; border-bottom: 1px solid #e2e8f0; margin-bottom: 30px; justify-content: flex-end; }
        .stTabs [data-baseweb="tab"] { height: 50px; background-color: transparent; border-radius: 0; font-weight: 600; color: #64748b; font-size: 1.05rem; padding: 0 10px; transition: all 0.2s; border: none !important; }
        .stTabs [aria-selected="true"] { color: #2563eb !important; border-bottom: 3px solid #2563eb !important; }
        
        .kpi-card { background: white; border-radius: 12px; padding: 20px 24px; box-shadow: 0 4px 20px rgba(0,0,0,0.03); border: 1px solid #f1f5f9; position: relative; overflow: hidden; display: flex; justify-content: space-between; align-items: center; }
        .kpi-card-border { position: absolute; left: 0; top: 0; bottom: 0; width: 5px; }
        .kpi-text-box { padding-left: 10px; }
        .kpi-label { font-size: 0.75rem; color: #64748b; font-weight: 700; text-transform: uppercase; letter-spacing: 0.5px; margin-bottom: 8px; }
        .kpi-value { font-size: 2rem; font-weight: 800; }
        .kpi-icon-box { width: 50px; height: 50px; border-radius: 12px; display: flex; align-items: center; justify-content: center; font-size: 1.5rem; }
        
        .home-card { background: white; border-radius: 20px; padding: 35px 20px; text-align: center; box-shadow: 0 10px 40px rgba(0,0,0,0.04); border: 1px solid #f8fafc; margin-bottom: 20px; transition: transform 0.2s; }
        .home-card:hover { transform: translateY(-5px); }
        .home-icon-wrap { width: 60px; height: 60px; border-radius: 16px; display: inline-flex; align-items: center; justify-content: center; font-size: 1.6rem; margin-bottom: 15px; }
        
        .chart-card { background: #ffffff; border-radius: 16px; padding: 24px; box-shadow: 0 4px 20px rgba(0,0,0,0.03); border: 1px solid #f1f5f9; margin-bottom: 24px; }
        .section-title { font-size: 1.2rem; font-weight: 800; color: #0f172a; margin-bottom: 1.5rem; display: flex; align-items: center; }
        .section-title i { margin-right: 10px; color: #2563eb; }
        
        div[data-testid="stChatMessage"] { background-color: transparent !important; }
        div[data-testid="stChatInput"] { border-radius: 16px; border: 1px solid #cbd5e1; padding: 5px; box-shadow: 0 10px 20px rgba(0,0,0,0.02); }
        .filter-header { font-size: 0.8rem; color: #94a3b8; font-weight: 800; text-transform: uppercase; letter-spacing: 1px; margin-bottom: 10px; }
    </style>
    """, unsafe_allow_html=True)
    
    st.markdown("""
    <div style="position: absolute; top: 15px; left: 40px; z-index: 999; display: flex; align-items: center; gap: 12px;">
        <div style="width: 38px; height: 38px; background: #2563eb; border-radius: 10px; display: flex; align-items: center; justify-content: center; color: white; box-shadow: 0 4px 10px rgba(37, 99, 235, 0.3);">
            <i class="fas fa-brain" style="font-size: 1.2rem;"></i>
        </div>
        <h1 style="margin: 0; font-size: 1.4rem; font-weight: 800; color: #2563eb; letter-spacing: -0.02em;">VietnamWorks Analytics</h1>
    </div>
    """, unsafe_allow_html=True)

def render_dash_kpi(title, value, icon, color_hex, bg_hex):
    html = f"""<div class="kpi-card"><div class="kpi-card-border" style="background-color: {color_hex};"></div><div class="kpi-text-box"><div class="kpi-label">{title}</div><div class="kpi-value" style="color: {color_hex};">{value}</div></div><div class="kpi-icon-box" style="background-color: {bg_hex}; color: {color_hex};"><i class="{icon}"></i></div></div>"""
    st.markdown(html, unsafe_allow_html=True)

def render_home_kpi(value, title, icon, color_hex, bg_hex):
    return f"""<div class="home-card"><div class="home-icon-wrap" style="background-color: {bg_hex}; color: {color_hex};"><i class="{icon}"></i></div><h3 style="margin: 0; font-size: 2.2rem; font-weight: 900; color: #0f172a;">{value}</h3><p style="margin: 8px 0 0 0; font-size: 0.8rem; font-weight: 700; color: #64748b; text-transform: uppercase; letter-spacing: 1px;">{title}</p></div>"""

def apply_modern_layout(fig):
    fig.update_layout(font_family="Plus Jakarta Sans", paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)", margin=dict(l=10, r=10, t=30, b=10))
    fig.update_xaxes(showgrid=False, zeroline=False, title_font=dict(size=12, color='#64748b'), tickfont=dict(color='#64748b'))
    fig.update_yaxes(showgrid=True, gridcolor='#f1f5f9', zeroline=False, title_font=dict(size=12, color='#64748b'), tickfont=dict(color='#64748b'))
    return fig

def render_overview_tab():
    st.markdown("<br>", unsafe_allow_html=True)
    col_text, col_img = st.columns([1.2, 1])
    
    with col_text:
        st.markdown("""
        <div style="padding: 20px 40px;">
            <span style="background-color: #eff6ff; color: #3b82f6; padding: 6px 14px; border-radius: 20px; font-size: 0.75rem; font-weight: 800; letter-spacing: 1px;">DATA SCIENCE PROJECT</span>
            <h1 style="font-size: 4rem; font-weight: 900; color: #0f172a; line-height: 1.1; margin-top: 25px;">
                Understanding<br>Recruitment by <span style="color: #2563eb;">Data</span>
            </h1>
            <p style="font-size: 1.15rem; color: #64748b; margin-top: 25px; margin-bottom: 35px; line-height: 1.6; max-width: 90%;">
                Hệ thống phân tích sâu dữ liệu thị trường việc làm, sử dụng công nghệ Hybrid Search và Mean Pooling Embedding tiên tiến nhất để định vị nhân tài và dự đoán xu hướng tuyển dụng.
            </p>
        </div>
        """, unsafe_allow_html=True)
        c1, c2, c3 = st.columns([1.5, 1.5, 2])
        with c1: st.button("View Dashboard 📈", type="primary", use_container_width=True)
        with c2: st.button("Run Prediction 🤖", use_container_width=True)

    with col_img:
        r1c1, r1c2 = st.columns(2)
        with r1c1: st.markdown(render_home_kpi("150k+", "Records", "fas fa-database", "#6366f1", "#e0e7ff"), unsafe_allow_html=True)
        with r1c2: st.markdown(render_home_kpi("IT & Tech", "Target Groups", "fas fa-user-check", "#10b981", "#d1fae5"), unsafe_allow_html=True)
        r2c1, r2c2 = st.columns(2)
        with r2c1: st.markdown(render_home_kpi("HyDE AI", "Core Engine", "fas fa-robot", "#f43f5e", "#ffe4e6"), unsafe_allow_html=True)
        with r2c2: st.markdown(render_home_kpi("50+", "Columns", "fas fa-columns", "#f59e0b", "#fef3c7"), unsafe_allow_html=True)

def render_dashboard_tab():
    list_industries, list_categories, list_levels = get_filter_options()
    st.markdown("""<div style="padding-left: 10px;"><h2 style="margin:0; font-size: 1.8rem; font-weight: 800; color: #0f172a;">Analytical Dashboard</h2><p style="color: #64748b; margin-top: 5px;">Real-time data visualization based on VietnamWorks survey</p></div><br>""", unsafe_allow_html=True)
    col_filter, col_main = st.columns([1, 4.5])
    
    with col_filter:
        st.markdown('<div class="filter-header">DATA FILTERS</div>', unsafe_allow_html=True)
        selected_industry = st.selectbox("INDUSTRY", list_industries)
        selected_category = st.selectbox("PROFESSION", list_categories)
        selected_level = st.selectbox("LEVEL", list_levels)
        st.markdown("<br>", unsafe_allow_html=True)
        if st.button("Apply Filters", use_container_width=True, type="primary"): load_dashboard_data.clear()

    with col_main:
        try:
            with st.spinner("Fetching Data Warehouse..."):
                metrics, df_skills, df_salary, df_treemap, df_companies, df_levels, df_cat_salary = load_dashboard_data(selected_industry, selected_category, selected_level)
            c1, c2, c3, c4 = st.columns(4)
            with c1: render_dash_kpi("TOTAL JOBS", f"{metrics['total_jobs']:,}", "fas fa-briefcase", "#f43f5e", "#ffe4e6")
            with c2: render_dash_kpi("COMPANIES", f"{metrics['total_companies']:,}", "fas fa-building", "#f59e0b", "#fef3c7")
            with c3: render_dash_kpi("AVG SALARY", f"{metrics['avg_salary'] / 1000000:.1f} M" if pd.notna(metrics['avg_salary']) else "N/A", "fas fa-money-bill-wave", "#10b981", "#d1fae5")
            with c4: render_dash_kpi("TOP SKILL", str(df_skills.iloc[0]['skill_name']) if not df_skills.empty else "N/A", "fas fa-star", "#3b82f6", "#eff6ff")
            st.markdown("<br>", unsafe_allow_html=True)
            
            r1c1, r1c2 = st.columns([1, 1])
            with r1c1:
                st.markdown('<div class="chart-card"><div class="section-title"><i class="fas fa-chart-pie"></i> Level Distribution</div>', unsafe_allow_html=True)
                st.plotly_chart(apply_modern_layout(px.pie(df_levels, values='job_count', names='job_level', hole=0.5, color_discrete_sequence=TAILWIND_COLORS).update_traces(textposition='inside', textinfo='percent', hoverinfo='label+percent', marker=dict(line=dict(color='#ffffff', width=3))).update_layout(height=300, showlegend=True, legend=dict(orientation="h", yanchor="bottom", y=-0.2, xanchor="center", x=0.5))), use_container_width=True)
                st.markdown('</div>', unsafe_allow_html=True)
            with r1c2:
                st.markdown('<div class="chart-card"><div class="section-title"><i class="fas fa-chart-bar"></i> Top Skills Demand</div>', unsafe_allow_html=True)
                st.plotly_chart(apply_modern_layout(px.bar(df_skills, x='job_count', y='skill_name', orientation='h').update_traces(marker_color='#2563eb', marker_line_color='#1e40af', marker_line_width=1).update_layout(yaxis={'categoryorder':'total ascending'}, height=300)), use_container_width=True)
                st.markdown('</div>', unsafe_allow_html=True)

            st.markdown('<div class="chart-card"><div class="section-title"><i class="fas fa-sitemap"></i> Market Distribution Treemap</div>', unsafe_allow_html=True)
            if not df_treemap.empty:
                st.plotly_chart(px.treemap(df_treemap, path=[px.Constant("Market"), 'industry_name', 'category_name'], values='job_count', color='industry_name', color_discrete_sequence=px.colors.qualitative.Pastel).update_traces(textinfo="label+value", textfont=dict(family="Plus Jakarta Sans", size=14), root_color="#f8fafc").update_layout(margin=dict(t=10, l=10, r=10, b=10), height=450), use_container_width=True)
            st.markdown('</div>', unsafe_allow_html=True)
        except Exception as e: st.error(f"Lỗi Data: {e}")

def render_prediction_tab():
    if "chat_sessions" not in st.session_state:
        st.session_state.chat_sessions = {"Chat Session 1": []}
        st.session_state.current_session = "Chat Session 1"
    if "cv_text" not in st.session_state:
        st.session_state.cv_text = ""

    col_menu_chat, col_main_chat = st.columns([1, 4.5])
    
    with col_menu_chat:
        st.markdown('<div class="filter-header">SESSIONS</div>', unsafe_allow_html=True)
        if st.button("➕ New Session", use_container_width=True):
            new_name = f"Chat Session {len(st.session_state.chat_sessions) + 1}"
            st.session_state.chat_sessions[new_name] = []
            st.session_state.current_session = new_name
            st.rerun()
            
        for session_name in reversed(list(st.session_state.chat_sessions.keys())):
            btn_style = "primary" if session_name == st.session_state.current_session else "secondary"
            if st.button(f"💬 {session_name}", key=session_name, use_container_width=True, type=btn_style):
                st.session_state.current_session = session_name
                st.rerun()

        st.markdown("<hr style='border-color: #e2e8f0;'>", unsafe_allow_html=True)
        st.markdown('<div class="filter-header">UPLOAD CV</div>', unsafe_allow_html=True)
        uploaded_file = st.file_uploader("Upload PDF", type=["pdf"], label_visibility="collapsed")
        if uploaded_file:
            with st.spinner("Reading CV..."): st.session_state.cv_text = extract_text_from_pdf(uploaded_file)
            st.success("✅ CV Loaded")

    with col_main_chat:
        current_history = st.session_state.chat_sessions[st.session_state.current_session]
        chat_container = st.container(height=650, border=True)
        
        with chat_container:
            if not current_history:
                with st.chat_message("assistant", avatar="🧠"):
                    st.markdown("### 👋 Hello!\nI am the **AI Recruitment Predictor**. Powered by Multi-Query Embedding & Hybrid SQL Search.\n\nEnter a job title, location, or upload your CV to start searching!")
            
            for message in current_history:
                st.chat_message(message["role"], avatar="🧠" if message["role"] == "assistant" else "👤").markdown(message["content"])

        if user_query := st.chat_input("E.g., Find AI Engineer roles in Hanoi..."):
            current_history.append({"role": "user", "content": user_query})
            with chat_container:
                st.chat_message("user", avatar="👤").markdown(user_query)
                with st.chat_message("assistant", avatar="🧠"):
                    with st.spinner("AI is running Hybrid Search (Vector + Exact Match)..."):
                        jobs_df, is_fallback, req_loc = fetch_and_rank_jobs(user_query, st.session_state.cv_text, top_k=10)
                        
                        if jobs_df.empty:
                            if is_fallback:
                                llm_reply = f"No exact matches found in **{req_loc.title()}**. Please try expanding your location or job title."
                            else:
                                llm_reply = "No matching jobs found. Please try different keywords."
                            st.markdown(llm_reply)
                        else:
                            llm_reply = generate_llm_response(user_query, jobs_df, st.session_state.cv_text, current_history[:-1], is_fallback, req_loc)
                            st.markdown(llm_reply)
                        
            current_history.append({"role": "assistant", "content": llm_reply})