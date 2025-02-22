# 📄 MySQL Schema - Logistics Database

## 📖 Giới thiệu
Schema này chứa cấu trúc dữ liệu của **Logistics Database**. Dữ liệu từ MySQL được trích xuất bằng **Debezium MySQL Connector** thông qua **Kafka Connect** và lưu trữ trên **HDFS** & **ClickHouse** để xử lý.

---

## 🔗 MySQL CDC với Kafka Connect (Debezium)
Để ingest dữ liệu từ MySQL bằng **Kafka UI**, làm theo các bước sau:

### 1️⃣ Truy cập Kafka UI
- Mở trình duyệt và truy cập **Kafka UI** (ví dụ: `http://localhost:9090`).
- Điều hướng đến mục **Connectors**.

### 2️⃣ Tạo Connector Mới
- Nhấn vào **Create Connector**.
- Chọn **Debezium MySQL Connector**.

### 3️⃣ Cấu Hình Connector
- Sao chép cấu hình dưới đây vào Kafka UI:

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

## 📂 Schema Chi Tiết

```sql
DROP DATABASE IF EXISTS logistics;
CREATE DATABASE logistics;

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

CREATE TABLE Drivers (
    driver_id INT PRIMARY KEY AUTO_INCREMENT,
    user_id INT UNIQUE,
    vehicle_license_plate VARCHAR(20) NOT NULL,
    vehicle_type VARCHAR(50),
    vehicle_year INT,
    FOREIGN KEY (user_id) REFERENCES Users(user_id)
);

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

CREATE TABLE Payments (
    payment_id INT PRIMARY KEY AUTO_INCREMENT,
    order_id INT,
    amount DECIMAL(10,2) NOT NULL,
    payment_method ENUM('credit_card', 'e_wallet', 'bank_transfer') NOT NULL,
    payment_status ENUM('pending', 'completed', 'failed') DEFAULT 'pending',
    payment_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (order_id) REFERENCES Orders(order_id)
);

CREATE TABLE Notifications (
    notification_id INT PRIMARY KEY AUTO_INCREMENT,
    user_id INT,
    message TEXT NOT NULL,
    notification_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (user_id) REFERENCES Users(user_id)
);
