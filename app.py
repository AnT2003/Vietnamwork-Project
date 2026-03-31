import sys

# 🟢 VŨ KHÍ TỐI THƯỢNG: MOCK THƯ VIỆN STREAMLIT 
# Đoạn code này đánh lừa Python, tạo ra một thư viện Streamlit "giả" vô hại.
# Điều này giúp file ai_engine.py của bạn giữ nguyên được các thẻ @st.cache_resource 
# mà không hề làm sập hệ thống Flask. Tuyệt đối tuân thủ yêu cầu không sửa ai_engine.py!
class MockStreamlit:
    def __getattr__(self, name):
        def mock_func(*args, **kwargs):
            if len(args) == 1 and callable(args[0]):
                return args[0]
            return lambda func: func
        return mock_func

sys.modules['streamlit'] = MockStreamlit()

# ----------------------------------------------------------------------
# TỪ ĐÂY TRỞ XUỐNG LÀ KHUNG FLASK BÌNH THƯỜNG
from flask import Flask, render_template, request, jsonify
from core_engine.dashboard_engine import get_filter_options, load_dashboard_data_json
from core_engine.ai_engine import fetch_and_rank_jobs, generate_llm_response, extract_text_from_pdf
from core_engine import session_manager
import traceback

app = Flask(__name__)
app.config['TEMPLATES_AUTO_RELOAD'] = True

# Khởi tạo Database cho lịch sử Chat
session_manager.init_db()

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/dashboard')
def dashboard():
    ind, cat, lvl = get_filter_options()
    return render_template('dashboard.html', industries=ind, categories=cat, levels=lvl)

@app.route('/chat')
def chat():
    return render_template('chat.html')

# ==========================================
# API TRUY XUẤT DỮ LIỆU DASHBOARD (Đã khôi phục)
# ==========================================
@app.route('/api/dashboard_data')
def api_dashboard():
    ind = request.args.get('industry', 'All')
    cat = request.args.get('category', 'All')
    lvl = request.args.get('level', 'All')
    data = load_dashboard_data_json(ind, cat, lvl)
    return jsonify(data)

# ==========================================
# CÁC API QUẢN LÝ LỊCH SỬ CHAT (SESSION)
# ==========================================
@app.route('/api/sessions', methods=['GET'])
def get_sessions():
    return jsonify(session_manager.get_all_sessions())

@app.route('/api/sessions/<session_id>', methods=['GET'])
def get_session_details(session_id):
    return jsonify(session_manager.get_session_messages(session_id))

@app.route('/api/sessions/<session_id>', methods=['DELETE'])
def delete_session(session_id):
    session_manager.delete_session(session_id)
    return jsonify({"status": "success"})

@app.route('/api/ai_chat', methods=['POST'])
def api_chat():
    try:
        user_query = request.form.get('query', '')
        session_id = request.form.get('session_id', '')
        cv_text = request.form.get('cv_text', '')
        
        # Xử lý file CV nếu có
        if 'cv_file' in request.files:
            cv_file = request.files['cv_file']
            if cv_file.filename != '' and cv_file.filename.endswith('.pdf'):
                cv_text = extract_text_from_pdf(cv_file)
        
        # 1. Tự động tạo Session mới nếu chưa có
        if not session_id or session_id == 'null':
            title = " ".join(user_query.split()[:6]) + "..." 
            session_id = session_manager.create_session(title)

        # 2. Lấy lịch sử chat từ Database
        db_history = session_manager.get_session_messages(session_id)
        chat_history = [{'role': m['role'], 'content': m['content']} for m in db_history[-6:]] 
        
        if not cv_text and db_history:
            for m in reversed(db_history):
                if m['cv_text']:
                    cv_text = m['cv_text']
                    break
                    
        # Lưu câu hỏi vào DB
        session_manager.add_message(session_id, 'user', user_query, cv_text)

        # 4. Tìm việc và Gọi AI (Gọi trực tiếp từ ai_engine.py đã tối ưu của bạn)
        jobs_df, is_fallback, req_loc = fetch_and_rank_jobs(user_query, cv_text, top_k=10)
        reply = generate_llm_response(user_query, jobs_df, cv_text, chat_history, is_fallback, req_loc)
        
        # Lưu câu trả lời của AI vào DB
        session_manager.add_message(session_id, 'assistant', reply, "")
        
        # Trả thẳng RAW để frontend tự biên dịch thành HTML
        return jsonify({"reply": reply, "cv_text": cv_text, "session_id": session_id})
    
    except Exception as e:
        error_details = traceback.format_exc()
        print(error_details)
        return jsonify({"reply": f"⚠️ Lỗi: {str(e)}", "cv_text": "", "session_id": request.form.get('session_id', '')})

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=6868, debug=True, use_reloader=False)