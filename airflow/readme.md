# 📄 Apache Airflow - Logistics Data Pipeline

## 📖 Giới thiệu

Thư mục **airflow** chứa cấu hình và mã nguồn cần thiết để triển khai Apache Airflow cho việc điều phối các pipeline xử lý dữ liệu của dự án Logistics. Airflow được sử dụng để lên lịch và quản lý các job cho cả **dbt** và các ứng dụng **Spark**.

## 🛠 Các thành phần trong thư mục airflow

### 1. Dockerfile

- **Mô tả**: Tệp `Dockerfile` này được sử dụng để xây dựng một image dựa trên Apache Airflow, được mở rộng với cài đặt thêm **Spark**, **Java** và một số thư viện bổ sung cần thiết.
- **Mục đích**: 
  - Cài đặt Spark và Java cho các job chạy Spark trong Airflow.
  - Đảm bảo image chứa đủ các dependencies cần thiết cho quá trình chạy các DAGs của dự án.
- **Lưu ý**: Kiểm tra các lệnh cài đặt trong Dockerfile để biết chính xác các phiên bản và thư viện được cài đặt.

### 2. Thư mục dags

- **Mô tả**: Thư mục `dags/` chứa các DAG (Directed Acyclic Graphs) dùng để định nghĩa luồng công việc trong Airflow.
- **Cấu trúc DAGs**:
  - **dbt_logistics_dag**: DAG này chịu trách nhiệm chạy các job liên quan đến **dbt** (ví dụ: chạy lệnh `dbt run`, `dbt deps`). DAG này sẽ điều phối quá trình xử lý dữ liệu qua các model dbt.
  - **spark_logistics_dag**: DAG này chịu trách nhiệm chạy các ứng dụng **Spark** (ví dụ: ingest, transform, load dữ liệu). Các job Spark này có thể được lên lịch để thực hiện ETL theo định kỳ.
- **Mục đích**: Đảm bảo quá trình xử lý dữ liệu được tự động hóa, theo lịch và có thể giám sát qua giao diện của Airflow.

### 3. Requirements

- **Mô tả**: Tệp `requirements.txt` chứa danh sách các thư viện Python cần thiết để chạy các DAGs và các script hỗ trợ trong Airflow.
- **Mục đích**:
  - Cài đặt các package cần thiết để Airflow và các script của bạn có thể chạy đúng chức năng.
  - Các thư viện có thể bao gồm các gói hỗ trợ cho Spark, xử lý dữ liệu, logging, và các công cụ bổ trợ khác.
- **Cài đặt**: Bạn có thể cài đặt các thư viện này bằng lệnh:
  ```bash
  pip install "apache-airflow==${AIRFLOW_VERSION}" -r /requirements.txt
