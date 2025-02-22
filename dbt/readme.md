# 📄 DBT

## 📖 Giới thiệu
Dữ liệu được trích xuất từ **MySQL** thông qua **Kafka Connect**, lưu trữ trên **ClickHouse** và được xử lý bởi **dbt**.

## 🏗 Models & Materialization

### 📂 Models (`models/`)
Thư mục `models/` chứa các tập hợp models giúp tổ chức các bảng dim và fact:

#### 🔹 Staging Layer (`staging/`)
- Chuẩn hóa dữ liệu từ MySQL.
- Làm sạch dữ liệu, đổi tên cột.
- Lọc dữ liệu theo thời gian.

#### 🔹 Dimension Layer (`dimension/`)
- Xây dựng các bảng dimension để lưu trữ thông tin mô tả thực thể.
- **`dim_users và dim_drivers`**: Áp dụng **Slowly Changing Dimension Type 2** (SCD Type 2) để theo dõi lịch sử thay đổi.
- **`dim_date và dim_locations`**: Áp dụng **Append Strategy** để bổ sung dữ liệu mới mà không cập nhật dữ liệu cũ.

### 🔹 Macros (`macros/`)
Thư mục `macros/` chứa các macro hỗ trợ trong dbt:
- **Macro xử lý SCD Type 2** để theo dõi thay đổi dữ liệu lịch sử.
- **Các Macro để hỗ trợ cho SCD type 2**.

## 🚀 Cách chạy dbt
### 1️⃣ Kiểm tra kết nối
```bash
 dbt debug --profiles-dir D:\hadoop\dbt\.dbt
```
### 2️⃣ Chạy toàn bộ models
```bash
dbt run --profiles-dir D:\hadoop\dbt\.dbt\
```
### 3️⃣ Chạy riêng models SCD Type 2
```bash
dbt run --profiles-dir D:\hadoop\dbt\.dbt\ -s dim_users 
```
