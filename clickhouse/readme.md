# 📄 ClickHouse - Query Scripts for Logistics Data Warehouse

## 📖 Giới thiệu

Thư mục này chứa các câu lệnh SQL dùng để tạo cấu trúc cơ sở dữ liệu trong ClickHouse, nhằm phục vụ việc lưu trữ và xử lý dữ liệu cho Logistics Data Warehouse. Cấu trúc thư mục bao gồm hai phần chính:

- **clickhouse-creating-tables**: Chứa các query dùng để tạo các bảng để hứng dữ liệu từ **ClickHouse Sink Connector**.
- **creating-views**: Chứa các query dùng để tạo view cho các bảng như `dim_date` và `dim_locations`, phục vụ cho các công cụ báo cáo như Power BI.

---

## 📂 Chi tiết thư mục

### 1. clickhouse-creating-tables

- **Mục đích**: Tạo các bảng trong ClickHouse để hứng dữ liệu từ **ClickHouse Sink Connector**.
- **Nội dung**: Bao gồm các file SQL với các câu lệnh `CREATE TABLE` để định nghĩa các bảng cần thiết, thiết lập các trường, kiểu dữ liệu và cấu hình engine (ví dụ: MergeTree, phân vùng theo ngày,...).
- **Hướng dẫn sử dụng**:
  - Kết nối đến ClickHouse trên local bằng client (clickhouse-client hoặc giao diện web).
  - Chạy các file SQL trong thư mục này theo thứ tự cần thiết (nếu có sự phụ thuộc giữa các bảng).

### 2. creating-views

- **Mục đích**: Tạo các view cho các bảng dimension như `dim_date` và `dim_locations`, nhằm tối ưu hóa việc truy xuất dữ liệu cho các công cụ báo cáo như Power BI.
- **Nội dung**: Bao gồm các file SQL chứa các câu lệnh `CREATE VIEW` để định nghĩa view dựa trên các bảng đã được tạo ở phần trên.
- **Hướng dẫn sử dụng**:
  - Sau khi các bảng đã được tạo, chạy các file SQL trong thư mục này để tạo các view.
  - Kiểm tra các view bằng cách thực hiện các truy vấn để đảm bảo dữ liệu được hiển thị chính xác.

---

## 📌 Kết luận

Thư mục **clickhouse** cung cấp các script cần thiết để tạo cấu trúc dữ liệu và view hỗ trợ cho Logistics Data Warehouse trong ClickHouse:
- **clickhouse-creating-tables**: Dùng để tạo các bảng lưu trữ dữ liệu được ingest từ **ClickHouse Sink Connector**.
- **creating-views**: Dùng để tạo view cho các bảng dimension, phục vụ cho báo cáo và phân tích dữ liệu (ví dụ: Power BI).

