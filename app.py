from flask import Flask, render_template, request, jsonify
from core_engine.dashboard_engine import get_filter_options, load_dashboard_data_json
from core_engine.ai_engine import fetch_and_rank_jobs, generate_llm_response, extract_text_from_pdf
import json
import traceback
import re

app = Flask(__name__)
app.config['TEMPLATES_AUTO_RELOAD'] = True

# 🟢 THUẬT TOÁN ĐẶC TRỊ: Tái tạo lại Bảng bị AI ngắt dòng sai
def fix_ai_broken_table(text):
    # 1. Tách các bảng bị dính liền (VD: "Link | | 1 |" -> "Link |\n| 1 |")
    text = re.sub(r'\|\s*\|', '|\n|', text)
    
    lines = text.split('\n')
    fixed_lines = []
    
    in_table = False
    expected_pipes = 0
    row_buffer = []
    current_pipes = 0
    
    for line in lines:
        line_stripped = line.strip()
        if not in_table:
            # Phát hiện bắt đầu bảng (Dòng có >= 3 dấu |)
            if line_stripped.startswith('|') and line_stripped.count('|') >= 3:
                in_table = True
                expected_pipes = line.count('|')
                row_buffer = [line]
                current_pipes = expected_pipes
                
                if current_pipes >= expected_pipes:
                    fixed_lines.append(line)
                    row_buffer = []
                    current_pipes = 0
            else:
                fixed_lines.append(line)
        else:
            # Đang nằm trong bảng
            if line_stripped == "" or (not line_stripped.startswith('|') and current_pipes == 0):
                # Kết thúc bảng
                in_table = False
                if row_buffer: fixed_lines.append("<br>".join(row_buffer))
                fixed_lines.append(line)
                row_buffer = []
                current_pipes = 0
                continue
                
            # Đưa dòng bị rớt vào bộ đệm và đếm dấu |
            row_buffer.append(line)
            current_pipes += line.count('|')
            
            # Đủ dấu | => Hàng đã hoàn thiện, ghép lại bằng <br>
            if current_pipes >= expected_pipes:
                fixed_lines.append("<br>".join(row_buffer))
                row_buffer = []
                current_pipes = 0
                
    if row_buffer: fixed_lines.append("<br>".join(row_buffer))
    return '\n'.join(fixed_lines)


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

@app.route('/api/dashboard_data')
def api_dashboard():
    ind = request.args.get('industry', 'All')
    cat = request.args.get('category', 'All')
    lvl = request.args.get('level', 'All')
    data = load_dashboard_data_json(ind, cat, lvl)
    return jsonify(data)

@app.route('/api/ai_chat', methods=['POST'])
def api_chat():
    try:
        if request.is_json:
            req = request.json
            user_query = req.get('query', '')
            chat_history = req.get('history', [])
            cv_text = req.get('cv_text', '')
        else:
            user_query = request.form.get('query', '')
            chat_history = json.loads(request.form.get('history', '[]'))
            cv_text = request.form.get('cv_text', '')
            
            if 'cv_file' in request.files:
                cv_file = request.files['cv_file']
                if cv_file.filename != '' and cv_file.filename.endswith('.pdf'):
                    cv_text = extract_text_from_pdf(cv_file)
        
        jobs_df, is_fallback, req_loc = fetch_and_rank_jobs(user_query, cv_text, top_k=10)
        
        if jobs_df.empty:
            reply = f"Rất tiếc, hệ thống không tìm thấy kết quả tại {req_loc}." if is_fallback else "Không tìm thấy công việc phù hợp."
        else:
            reply = generate_llm_response(user_query, jobs_df, cv_text, chat_history, is_fallback, req_loc)
        
        # 🟢 GỌI THUẬT TOÁN VÁ BẢNG TRƯỚC KHI TRẢ VỀ CHO WEB
        fixed_reply = fix_ai_broken_table(reply)
        
        return jsonify({"reply": fixed_reply, "cv_text": cv_text})
        
    except Exception as e:
        error_details = traceback.format_exc()
        print(f"\n--- LỖI SERVER NGHIÊM TRỌNG ---\n{error_details}\n-----------------------------\n")
        return jsonify({"reply": f"⚠️ **Lỗi Server Python:** {str(e)}", "cv_text": ""})

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=6868, debug=True, use_reloader=False)