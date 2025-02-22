# 📄 MySQL Schema - Logistics Database

## 📖 Giới thiệu
Schema này chứa cấu trúc dữ liệu của **Logistics Database**. Dữ liệu từ MySQL sẽ được trích xuất qua **Kafka Connect CDC** và lưu trữ trên **HDFS** & **ClickHouse** để xử lý.

---

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
