-------------------------------------------------------------
# USERS


create table `logistics_src.logistics.Users`
(

  `after` Tuple
  (
    user_id Int
   	, full_name String
    , email String
    , password_hash String
    , phone_number String
    , address String
    , role String
    , created_at String
  )
  , `source` Tuple(ts_ms UInt64)
  , op String
)
engine = MergeTree
order by `after`.user_id;


drop table `logistics_src.logistics.Users`;

select * from `logistics_src.logistics.Users`;




-------------------------------------------------------------
# DRIVERS


create table `logistics_src.logistics.Drivers`
(

  `after` Tuple
  (
    driver_id Int
   	, user_id Int
    , vehicle_license_plate String
    , vehicle_type String
    , vehicle_year INT
  )
  , `source` Tuple(ts_ms UInt64)
  , op String
)
engine = MergeTree
order by `after`.driver_id;

drop table `logistics_src.logistics.Drivers`;

select * from `logistics_src.logistics.Drivers`;




-------------------------------------------------------------
# ORDERS


create table `logistics_src.logistics.Orders`
(

  `after` Tuple
  (
    order_id Int
   	, user_id Int
    , pickup_address String
    , delivery_address String
    , package_description String
    , package_weight FLOAT
    , delivery_time String
    , status Enum('processing', 'accepted', 'in_transit', 'delivered')
    , created_at String
  )
  , `source` Tuple(ts_ms UInt64)
  , op String
)
engine = MergeTree
order by `after`.order_id;

drop table `logistics_src.logistics.Orders`;

select * from `logistics_src.logistics.Orders`;




-------------------------------------------------------------
# SHIPMENTS


create table `logistics_src.logistics.Shipments`
(

  `after` Tuple
  (
    shipment_id Int
   	, order_id Int
    , driver_id Int
    , current_location String
    , estimated_delivery_time String
    , `status` Enum('assigned', 'in_transit', 'completed')
  )
  , `source` Tuple(ts_ms UInt64)
  , op String
)
engine = MergeTree
order by `after`.shipment_id;

drop table `logistics_src.logistics.Shipments`;

select * from `logistics_src.logistics.Shipments`;





-------------------------------------------------------------
# PAYMENTS


create table `logistics_src.logistics.Payments`
(

  `after` Tuple
  (
    payment_id Int
   	, order_id Int
    , amount Decimal(10, 2)
    , payment_method Enum('credit_card', 'e_wallet', 'bank_transfer')
    , payment_status Enum('pending', 'completed', 'failed')
    , payment_date String
  )
  , `source` Tuple(ts_ms UInt64)
  , op String
)
engine = MergeTree
order by `after`.payment_id;

drop table `logistics_src.logistics.Payments`;

select * from `logistics_src.logistics.Payments`;

