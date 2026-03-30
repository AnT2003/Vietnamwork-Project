FROM python:3.11.8-slim

WORKDIR /app

# Ép Python in Log ra màn hình ngay lập tức (Không bị nghẽn)
ENV PYTHONUNBUFFERED=1
ENV PYTHONPATH="/app"
ENV AIRFLOW_HOME="/app/airflow"
ENV AIRFLOW__WEBSERVER__WEB_SERVER_PORT=8999
ENV AIRFLOW__WEBSERVER__WEB_SERVER_HOST=0.0.0.0
ENV AIRFLOW__CORE__DAGS_FOLDER="/app/dags"
ENV AIRFLOW__CORE__LOAD_EXAMPLES="False"

RUN apt-get update && apt-get install -y \
    build-essential \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/*

COPY . .

RUN pip install --upgrade pip

# ÉP NUMPY DƯỚI 2.0 ĐỂ CỨU THƯ VIỆN PYVI
RUN pip install "numpy<2.0.0" pandas sqlalchemy psycopg2-binary sentence-transformers ollama python-dotenv PyPDF2 pyvi plotly requests apache-airflow==2.8.1 Flask Markdown "Flask-Session<0.6.0"

EXPOSE 6868 8999

CMD ["python", "app.py"]