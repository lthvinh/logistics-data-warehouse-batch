# 📝 MySQL Schema - Logistics Database

## 📚 Giới thiệu
Schema này chứa cấu trúc dữ liệu của **Logistics Database**. Dữ liệu từ MySQL được trích xuất bằng **Debezium MySQL Connector** thông qua **Kafka Connect** và lưu trữ trên **HDFS** & **ClickHouse** để xử lý.

---

## 🔗 MySQL CDC với Kafka Connect (Debezium)
👉 **Cách ingest dữ liệu từ MySQL bằng Kafka UI:**

### 1️⃣ Truy cập Kafka UI
- Mở trình duyệt và truy cập **Kafka UI** (ví dụ: `http://localhost:9090`).
- Điều hướng đến mục **Connectors**.

### 2️⃣ Tạo Connector Mới
- Nhấn vào **Create Connector**.

### 3️⃣ Cấu Hình Connector
- Sao chép cáu hình dưới đây vào Kafka UI:

```json
{
  "connector.class": "io.debezium.connector.mysql.MySqlConnector",
  "tasks.max": "1",
  "database.hostname": "mysql",
  "database.port": "3306",
  "database.server.id": "1",
  "database.user": "root",
  "database.password": "Vinh@123456",
  "schema.history.internal.kafka.bootstrap.servers": "kafka:9092",
  "topic.prefix": "logistics_src",
  "key.converter": "io.confluent.connect.avro.AvroConverter",
  "key.converter.schema.registry.url": "http://kafka-schema-registry:8081",
  "value.converter": "io.confluent.connect.avro.AvroConverter",
  "value.converter.schema.registry.url": "http://kafka-schema-registry:8081",
  "database.include.list": "logistics",
  "table.include.list": "logistics.Users,logistics.Orders,logistics.Drivers,logistics.Payments,logistics.Shipments",
  "include.schema.changes": "false",
  "schema.history.internal.kafka.topic": "schema-changes.logistics"
}
```

### 4️⃣ Lưu và Khởi Chạy
- Nhấn **Submit**.
- Kiểm tra trạng thái connector trong Kafka UI để đảm bảo nó hoạt động bình thường.

---

## 📂 Schema MySQL Logistics

### 1. Bảng `Users` - Quản lý người dùng
```sql
CREATE TABLE Users (
    user_id INT PRIMARY KEY AUTO_INCREMENT,
    full_name VARCHAR(255) NOT NULL,
    email VARCHAR(255) UNIQUE NOT NULL,
    password_hash VARCHAR(255) NOT NULL,
    phone_number VARCHAR(20) UNIQUE NOT NULL,
    address VARCHAR(255),
    role ENUM('user', 'driver') NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```
- Quản lý người dùng của hệ thống.
- `role`: Phân loại giữa **user** (khách hàng) và **driver** (tài xế).

### 2. Bảng `Drivers` - Quản lý tài xế
```sql
CREATE TABLE Drivers (
    driver_id INT PRIMARY KEY AUTO_INCREMENT,
    user_id INT UNIQUE,
    vehicle_license_plate VARCHAR(20) NOT NULL,
    vehicle_type VARCHAR(50),
    vehicle_year INT,
    FOREIGN KEY (user_id) REFERENCES Users(user_id)
);
```
- Liên kết với `Users`, chỉ các user là **driver** mới có thông tin trong bảng này.

### 3. Bảng `Orders` - Quản lý đơn hàng
```sql
CREATE TABLE Orders (
    order_id INT PRIMARY KEY AUTO_INCREMENT,
    user_id INT,
    pickup_address VARCHAR(255) NOT NULL,
    delivery_address VARCHAR(255) NOT NULL,
    package_description VARCHAR(255),
    package_weight FLOAT,
    delivery_time TIMESTAMP,
    status ENUM('processing', 'accepted', 'in_transit', 'delivered') DEFAULT 'processing',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (user_id) REFERENCES Users(user_id)
);
```
- Quản lý các đơn hàng từ **khách hàng**.
- `status`: Trạng thái giao hàng (**processing -> accepted -> in_transit -> delivered**).

### 4. Bảng `Shipments` - Quản lý giao hàng
```sql
CREATE TABLE Shipments (
    shipment_id INT PRIMARY KEY AUTO_INCREMENT,
    order_id INT,
    driver_id INT,
    current_location VARCHAR(255),
    estimated_delivery_time TIMESTAMP,
    status ENUM('assigned', 'in_transit', 'completed') DEFAULT 'assigned',
    FOREIGN KEY (order_id) REFERENCES Orders(order_id),
    FOREIGN KEY (driver_id) REFERENCES Drivers(driver_id)
);
```
- Quản lý quá trình giao hàng.
- Liên kết với **Orders** và **Drivers**.

### 5. Bảng `Payments` - Quản lý thanh toán
```sql
CREATE TABLE Payments (
    payment_id INT PRIMARY KEY AUTO_INCREMENT,
    order_id INT,
    amount DECIMAL(10,2) NOT NULL,
    payment_method ENUM('credit_card', 'e_wallet', 'bank_transfer') NOT NULL,
    payment_status ENUM('pending', 'completed', 'failed') DEFAULT 'pending',
    payment_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (order_id) REFERENCES Orders(order_id)
);
```
- Ghi nhận thông tin thanh toán các đơn hàng.

### 6. Bảng `Notifications` - Quản lý thông báo
```sql
CREATE TABLE Notifications (
    notification_id INT PRIMARY KEY AUTO_INCREMENT,
    user_id INT,
    message TEXT NOT NULL,
    notification_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (user_id) REFERENCES Users(user_id)
);
```
- Lưu thông tin thông báo gửi đến người dùng.

