Dưới đây là README.md hoàn chỉnh đã được cập nhật, bao gồm cả hình ảnh schema của source, schema của Data Warehouse và luồng ETL. Mình giả định rằng bạn đã lưu các hình ảnh vào thư mục `docs/images/` với tên file tương ứng. Bạn có thể điều chỉnh đường dẫn nếu cần:

---

```markdown
# 📄 Logistics Data Warehouse

## 📖 Giới thiệu

Project Logistics Data Warehouse xây dựng một hệ thống ETL toàn diện nhằm ingest, transform và lưu trữ dữ liệu phục vụ phân tích và báo cáo. Hệ thống sử dụng cùng một nguồn dữ liệu từ **MySQL** (với schema nguồn) và cùng một **Data Warehouse Schema** làm kết quả cuối cùng, nhưng ETL được triển khai theo hai hướng khác nhau:
- **ETL bằng dbt**: Sử dụng dbt để thực hiện transform dữ liệu.
- **ETL bằng Spark**: Sử dụng Apache Spark để thực hiện các bước làm sạch, chuyển đổi và load dữ liệu.

## 🔍 Data Source & Final Data Warehouse Schema

### 1. MySQL Schema (Source)
- **Vai trò**: Đây là nguồn dữ liệu gốc của hệ thống, chứa các bảng như: **Users**, **Drivers**, **Orders**, **Shipments**, **Payments**, **Notifications**.
- **Cách sử dụng**: Dữ liệu từ MySQL được trích xuất thông qua Kafka Connect (sử dụng Debezium MySQL Connector) và ingest vào hệ thống.

![Source Schema](source_schema.png)

### 2. Data Warehouse Schema (Final Result)
- **Vai trò**: Đây là schema cuối cùng của Data Warehouse, được xây dựng dựa trên dữ liệu đã được transform và tích hợp từ nguồn.
- **Thành phần**: Bao gồm các bảng dimension và fact (theo mô hình Kimball) được lưu trữ trong hệ thống lưu trữ phân tích (ClickHouse hoặc Delta Table thông qua Spark).

![Data Warehouse Schema](data_warehouse_schema.png)

## 🔄 Kiến trúc hệ thống

### Ingest & Processing
- **Nguồn dữ liệu**: MySQL (source schema).
- **ETL bằng Spark**:
  - **Raw Layer**: Dữ liệu được ingest từ Kafka bằng HDFS Sink Connector dưới dạng Avro, phân vùng theo ngày/tháng/năm.
  - **Enriched Layer**: Sử dụng Apache Spark để lọc, làm sạch và chuyển đổi dữ liệu, lưu dưới dạng Parquet.
  - **Curated Layer**: Spark tiếp tục xử lý để xây dựng Data Warehouse, lưu dưới dạng Delta Table.
- **ETL bằng dbt**:
  - Sử dụng dbt để thực hiện transform dữ liệu từ các bảng nguồn, áp dụng các kỹ thuật như Slowly Changing Dimension Type 2 (SCD Type 2) và Incremental Append, nhằm xây dựng các bảng dimension và fact trong Data Warehouse Schema.

### Orchestration
- **Apache Airflow**: Điều phối toàn bộ quy trình ETL, bao gồm việc chạy các job Spark và các lệnh dbt theo lịch trình.

### Analytical Storage
- **ClickHouse**: Lưu trữ và truy vấn dữ liệu ingest từ ClickHouse Sink Connector, cùng với các view hỗ trợ báo cáo Power BI.

![ETL Flow](etl_flow.svg)

## 📂 Cấu trúc dự án

### 1. Airflow
- **Dockerfile**: Image Apache Airflow mở rộng cài Spark, Java và các thư viện bổ sung.
- **dags/**: Chứa các DAG điều phối công việc:
  - `spark_logistics_dag`: DAG cho các Spark Application (ETL bằng Spark).
  - (Nếu áp dụng) `dbt_logistics_dag`: DAG điều phối các lệnh dbt.
- **requirements.txt**: Danh sách các thư viện Python cần thiết.

### 2. ClickHouse
- **clickhouse-creating-tables/**: Các file SQL tạo bảng trong ClickHouse để hứng dữ liệu từ ClickHouse Sink Connector.
- **creating-views/**: Các file SQL tạo view cho các bảng dimension (ví dụ: `dim_date`, `dim_locations`) phục vụ báo cáo Power BI.

### 3. ETL theo Spark
- **Raw Layer**: Ingest dữ liệu từ Kafka vào HDFS bằng HDFS Sink Connector (Avro, phân vùng theo thời gian).
- **Enriched Layer**: Sử dụng Spark để làm sạch và chuyển đổi dữ liệu, lưu dưới dạng Parquet.
- **Curated Layer**: Sử dụng Spark để tích hợp dữ liệu thành Data Warehouse, lưu dưới dạng Delta Table.

### 4. ETL theo dbt
- **Staging Layer**: Chuẩn hóa dữ liệu từ MySQL.
- **Dimension Layer**: Xây dựng các bảng dimension (ví dụ: `dim_users` với SCD Type 2, `dim_locations` với Append Strategy).
- **Fact Layer**: Xây dựng các bảng fact phục vụ phân tích.

### 5. MySQL Schema & Data Warehouse Schema
- **MySQL Schema (Source)**: Schema ban đầu chứa các bảng nguồn từ MySQL.
- **Data Warehouse Schema (Final Result)**: Schema sau khi tích hợp và transform, chứa các bảng dimension và fact phục vụ báo cáo.

## 🚀 Hướng dẫn vận hành

### Airflow
1. **Xây dựng Image Airflow**:  
   ```bash
   docker build -t my-airflow-image .
   ```
2. **Cài đặt các package Python**:
   ```bash
   pip install -r requirements.txt
   ```
3. **Khởi động Airflow**:  
   Đảm bảo các DAG (như `spark_logistics_dag`) được load và chạy đúng theo lịch trình.

### ClickHouse
1. **Kết nối đến ClickHouse trên local**:
   ```bash
   clickhouse-client --host localhost --port 8123 --user <username> --password <password>
   ```
2. **Tạo bảng và view**:
   - Chạy các file SQL trong thư mục `clickhouse-creating-tables` để tạo các bảng cần thiết.
   - Sau đó, chạy các file SQL trong thư mục `creating-views` để tạo view phục vụ báo cáo (Power BI).

### ETL bằng Spark
1. **Ingest dữ liệu (Raw Layer)**:  
   Cấu hình HDFS Sink Connector để ingest dữ liệu từ Kafka vào HDFS với định dạng Avro và phân vùng theo ngày, tháng, năm.
2. **Transform & Load**:
   - Chạy Spark job để chuyển đổi dữ liệu từ Raw → Enriched (Parquet).
   - Chạy Spark job để tạo Data Warehouse từ Enriched (Delta Table).

### ETL bằng dbt
1. **Cài đặt packages dbt**:
   ```bash
   dbt deps
   ```
2. **Chạy toàn bộ models**:
   ```bash
   dbt run
   ```
3. **Chạy riêng models SCD Type 2 (ví dụ: dim_users)**:
   ```bash
   dbt run --select dim_users
   ```

## 📌 Kết luận

Project Logistics Data Warehouse tích hợp các công nghệ hiện đại để xây dựng một hệ thống ETL toàn diện:
- **Source**: MySQL Schema, nơi dữ liệu gốc được lấy từ hệ thống MySQL.
- **ETL**: Được thực hiện qua hai hướng độc lập:
  - **Spark**: Xử lý dữ liệu từ Raw (Avro) → Enriched (Parquet) → Curated (Delta Table).
  - **dbt**: Transform dữ liệu từ các bảng nguồn để xây dựng các model dimension và fact.
- **Final Result**: Data Warehouse Schema, nơi lưu trữ dữ liệu đã được tích hợp và tối ưu cho báo cáo và phân tích.
- **Orchestration & Analytical Storage**: Apache Airflow đảm bảo quy trình ETL tự động và ClickHouse hỗ trợ truy vấn dữ liệu hiệu quả.
