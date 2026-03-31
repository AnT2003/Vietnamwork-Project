from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import timedelta
import pendulum

# 1. Cài đặt múi giờ chuẩn Việt Nam (UTC+7)
local_tz = pendulum.timezone("Asia/Ho_Chi_Minh")

# 2. Cấu hình mặc định của luồng chạy
default_args = {
    'owner': 'data_engineer',
    'depends_on_past': False,
    # Chọn một ngày bắt đầu ở quá khứ gần (để Airflow có thể kích hoạt ngay lịch tiếp theo)
    'start_date': pendulum.datetime(2026, 3, 27, tz=local_tz), 
    'retries': 1,                         # Nếu chạy lỗi (vd: rớt mạng), cho phép thử lại 1 lần
    'retry_delay': timedelta(minutes=5),  # Đợi 5 phút rồi mới thử lại
}

# 3. Khởi tạo DAG
with DAG(
    dag_id='vietnamworks_daily_pipeline',
    default_args=default_args,
    description='Tự động cào và nạp dữ liệu VietnamWorks mỗi ngày',
    schedule_interval='0 22 * * *',       # Cú pháp Cron: 0 phút, 22 giờ -> Đúng 10h đêm hằng ngày
    catchup=False,                        # Quan trọng: Không chạy bù các ngày cũ bị lỡ để tránh lặp data
    tags=['vietnamworks', 'ai_headhunter', 'etl'],
) as dag:

    # 4. Định nghĩa Task gọi file python của bạn
    run_daily_etl = BashOperator(
        task_id='trigger_master_pipeline',
        # Trỏ đúng đường dẫn tuyệt đối bên trong Docker container
        bash_command='python /app/dags/master_pipeline.py', 
    )
    # gọi thẳng task:
    run_daily_etl
