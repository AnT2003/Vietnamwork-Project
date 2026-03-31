#!/bin/bash

# 🟢 TUYỆT CHIÊU 1: Cấm Airflow đọc nhầm file AI
echo -e "scripts/.*\nmaster_pipeline.*\ninitial_load.*" > /app/dags/.airflowignore

export AIRFLOW__CORE__DAGS_FOLDER="/app/dags"
export AIRFLOW__CORE__LOAD_EXAMPLES="False"

echo "0. Dọn rác tiến trình cũ..."
rm -f /app/airflow/airflow-webserver.pid
rm -f /app/airflow/airflow-scheduler.pid

echo "1. Khởi tạo Database cho Airflow..."
airflow db init

echo "2. Tạo tài khoản Admin Airflow..."
airflow users create \
    --username admin \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email admin@example.com \
    --password admin || true

# 🟢 TUYỆT CHIÊU 2: Ép Airflow chỉ dùng 1 Worker để tiết kiệm tối đa RAM
echo "3. Khởi động Airflow Webserver (Chế độ tiết kiệm RAM)..."
nohup airflow webserver -p 8999 --workers 1 > /app/airflow-web.log 2>&1 &

echo "4. Khởi động Airflow Scheduler (Chạy ngầm an toàn)..."
nohup airflow scheduler > /app/airflow-scheduler.log 2>&1 &

echo "5. Khởi động ứng dụng Flask Web ở cổng 6868..."
exec python app.py