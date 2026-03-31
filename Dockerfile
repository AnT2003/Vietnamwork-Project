FROM python:3.11.8-slim

WORKDIR /app

# Các biến môi trường tối ưu
ENV PYTHONUNBUFFERED=1
ENV PYTHONPATH="/app"
ENV AIRFLOW_HOME="/app/airflow"

# Cài đặt các gói hệ thống cần thiết
RUN apt-get update && apt-get install -y \
    build-essential \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

COPY . .

RUN pip install --upgrade pip

# 🟢 ĐÃ THÊM LẠI "streamlit" VÀO ĐÂY ĐỂ AI_ENGINE.PY KHÔNG BỊ LỖI
RUN pip install "numpy<2.0.0" \
    pandas \
    sqlalchemy \
    psycopg2-binary \
    sentence-transformers \
    ollama \
    python-dotenv \
    PyPDF2 \
    pyvi \
    plotly \
    requests \
    apache-airflow==2.8.1 \
    Flask \
    Markdown \
    "Flask-Session<0.6.0" \
    streamlit

# Mở cổng 6868 cho Flask App và 8999 cho Airflow
EXPOSE 6868 8999

# Cấp quyền thực thi cho file start.sh
RUN chmod +x /app/start.sh

# Lệnh khởi chạy toàn bộ hệ thống (Airflow DB + Scheduler + Webserver + Flask)
CMD ["/app/start.sh"]