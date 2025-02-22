# Kafka Connect

## 📖 Giới thiệu
Tài liệu này mô tả hệ thống xử lý dữ liệu **Logistics Database** sử dụng **Kafka Connect** để ingest dữ liệu từ **MySQL** và lưu trữ trên **HDFS** & **ClickHouse**. Dữ liệu được trích xuất thông qua **Debezium MySQL Connector** và lưu vào sink bằng các sink connectors.

---

## 🔗 Kafka Connect Configuration

### 📂 Thư mục `kafka-connect`
Thư mục này chứa các plugin và cấu hình connector:

- **connect_plugins**:
  - `clickhouse-clickhouse-kafka-connect-v1.2.6`
  - `confluentinc-kafka-connect-hdfs3-1.2.2`
  - `debezium-connector-mysql`

- **connector_configuration**:
  - **ClickHouse Sink Connector**:
    ```json
    {
      "connector.class": "com.clickhouse.kafka.connect.ClickHouseSinkConnector",
      "tasks.max": "1",
      "topics.regex": "logistics_src.logistics.*",
      "ssl": "false",
      "hostname": "clickhouse",
      "port": "8123",
      "username": "vinh",
      "password": "Vinh@123456",
      "database": "logistics",
      "value.converter": "io.confluent.connect.avro.AvroConverter",
      "value.converter.schema.registry.url": "http://kafka-schema-registry:8081"
    }
    ```
  
  - **HDFS Sink Connector**:
    ```json
    {
      "connector.class": "io.confluent.connect.hdfs3.Hdfs3SinkConnector",
      "tasks.max": "1",
      "topics.regex": "logistics_src.logistics.*",
      "hdfs.url": "hdfs://hdfs-namenode:9000",
      "flush.size": "20",
      "rotate.interval.ms": "60000",
      "logs.dir": "/raw/transactional/mysql/logistics/logs",
      "topics.dir": "/raw/transactional/mysql/logistics/topics",
      "partitioner.class": "io.confluent.connect.storage.partitioner.TimeBasedPartitioner",
      "partition.duration.ms": "86400000",
      "locale": "en",
      "path.format": "'year'=YYYY/'month'=MM/'day'=dd",
      "timezone": "Asia/Ho_Chi_Minh",
      "key.converter": "io.confluent.connect.avro.AvroConverter",
      "key.converter.schema.registry.url": "http://kafka-schema-registry:8081",
      "value.converter": "io.confluent.connect.avro.AvroConverter",
      "value.converter.schema.registry.url": "http://kafka-schema-registry:8081",
      "format.class": "io.confluent.connect.hdfs3.avro.AvroFormat",
      "avro.codec": "snappy",
      "confluent.topic.bootstrap.servers": "kafka:9092",
      "confluent.topic.replication.factor": "1"
    }
    ```
  
  - **MySQL Source Connector**:
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

