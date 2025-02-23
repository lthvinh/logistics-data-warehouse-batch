# 📄 Data Pipeline - Logistics Data Warehouse

## 📖 Giới thiệu

Thư mục **src** chứa toàn bộ code cho pipeline xử lý dữ liệu của dự án. Quy trình này được xây dựng dựa trên Apache Spark và các connector để ingest, transform và load dữ liệu qua ba lớp chính:
- **Raw Layer**: Dữ liệu gốc được ingest từ Kafka vào HDFS dưới dạng Avro (phân vùng theo ngày, tháng, năm).
- **Enriched Layer**: Dữ liệu được xử lý, làm sạch và chuyển đổi bằng Spark, lưu dưới dạng Parquet.
- **Curated Layer**: Dữ liệu cuối cùng được xử lý thêm để tạo ra Data Warehouse, lưu dưới dạng Delta Table.

## 📂 Cấu trúc thư mục trong src

### 1. **config/**
- **Mô tả**: Chứa các file cấu hình cho Spark, logger, đường dẫn đến các lớp dữ liệu (raw, enriched, curated) cũng như các tham số ngày, tháng được sử dụng cho quá trình trích xuất dữ liệu.
- **Ví dụ**: File cấu hình có thể bao gồm thông tin về cluster Spark, định dạng file, đường dẫn HDFS, … 

### 2. **decorator/**
- **Mô tả**: Thư mục chứa các file Python định nghĩa decorator được sử dụng để trang trí (decorate) các phương thức và hàm trong toàn bộ code của thư mục src.
- **Mục đích**: Giúp thêm tính năng logging, kiểm soát hiệu năng hoặc các tính năng chung khác một cách nhất quán.

### 3. **extract/**
- **Mô tả**: Chứa các tệp thực hiện chức năng extract dữ liệu từ các layer khác nhau:
  - **raw_data_extractor**: Dùng để trích xuất dữ liệu từ Raw Layer (dữ liệu gốc dưới dạng Avro trên HDFS).
  - **enriched_data_extractor**: Dùng để trích xuất dữ liệu từ Enriched Layer.
- **Mục đích**: Đảm bảo dữ liệu được đọc từ nguồn một cách hiệu quả và sẵn sàng cho bước xử lý tiếp theo.

### 4. **transform/**
- **Mô tả**: Chứa các tệp xử lý (transform) dữ liệu:
  - **enriched_data_transformer**: Chuyển đổi dữ liệu từ Raw Layer thành dữ liệu Enriched (làm sạch, lọc, chuyển đổi định dạng, …).
  - **curated_data_transformer**: Chuyển đổi dữ liệu từ Enriched Layer thành dữ liệu Data Warehouse (Curated Layer), bao gồm các bước tổng hợp và định dạng dữ liệu cho báo cáo.
- **Mục đích**: Xử lý và chuyển đổi dữ liệu theo yêu cầu của doanh nghiệp, đảm bảo dữ liệu đạt chất lượng cao cho phân tích.

### 5. **load/**
- **Mô tả**: Chứa các tệp load dữ liệu đã được transform:
  - **enriched_data_loader**: Load dữ liệu đã transform từ Raw Layer vào Enriched Layer (lưu dưới dạng Parquet).
  - **curated_data_loader**: Load dữ liệu đã transform từ Enriched Layer vào Curated Layer (lưu dưới dạng Delta Table).
- **Mục đích**: Chuyển giao dữ liệu đã được xử lý vào các layer đích, sẵn sàng cho việc truy vấn và phân tích.

### 6. **workflows/**
- **Mô tả**: Chứa các Spark Application được sử dụng trong Airflow để lên lịch và quản lý quy trình ETL.
- **Mục đích**: Tích hợp các job Spark vào workflow của Airflow, đảm bảo quy trình chạy định kỳ và tự động.

## 🔄 Quy trình xử lý dữ liệu

1. **Raw Layer**:  
   - Dữ liệu được ingest từ Kafka bằng HDFS Sink Connector.
   - Lưu trữ ở dạng Avro, phân vùng theo ngày, tháng, năm trong HDFS.

2. **Enriched Layer**:  
   - Sử dụng Spark để đọc dữ liệu Avro từ Raw Layer.
   - Thực hiện các bước làm sạch và chuyển đổi dữ liệu.
   - Lưu trữ kết quả dưới dạng Parquet.
   - Lưu trữ ở dạng Avro, phân vùng theo ngày, tháng, năm trong HDFS.

3. **Curated Layer**:  
   - Tiếp tục xử lý dữ liệu từ Enriched Layer bằng Spark.
   - Tích hợp và tổng hợp dữ liệu nhằm xây dựng Data Warehouse.
   - Lưu trữ kết quả cuối cùng dưới dạng Delta Table.

## 🚀 Hướng dẫn triển khai

1. **Config**:  
   - Chỉnh sửa các file trong thư mục **config/** để thiết lập thông số Spark, logger, đường dẫn HDFS, và tham số thời gian cần thiết cho việc trích xuất dữ liệu.

2. **Extract**:  
   - Sử dụng các tệp trong **extract/** để đọc dữ liệu từ Raw hoặc Enriched Layer.

3. **Transform**:  
   - Chạy các Spark job sử dụng các tệp trong **transform/** để chuyển đổi dữ liệu từ Raw → Enriched và Enriched → Curated.

4. **Load**:  
   - Sử dụng các tệp trong **load/** để ghi dữ liệu đã transform vào Enriched Layer (Parquet) hoặc Curated Layer (Delta Table).

5. **Workflows**:  
   - Tích hợp các Spark Application có trong **workflows/** vào Airflow để tự động hóa và lên lịch quy trình ETL.

## 📌 Kết luận

Quy trình pipeline này đảm bảo dữ liệu được ingest, transform và load một cách hiệu quả từ nguồn đến Data Warehouse.  
- **Raw Layer**: Lưu trữ dữ liệu gốc dưới dạng Avro theo phân vùng thời gian.  
- **Enriched Layer**: Dữ liệu được làm sạch và chuyển đổi, lưu dưới dạng Parquet.  
- **Curated Layer**: Dữ liệu cuối cùng được tích hợp và lưu dưới dạng Delta Table, sẵn sàng cho báo cáo và phân tích.
