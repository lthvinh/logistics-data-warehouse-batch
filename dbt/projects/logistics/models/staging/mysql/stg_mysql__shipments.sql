with raw_data as (
    select
        *
    from
        {{ source('raw', 'logistics_src.logistics.Shipments') }}
)

, transformed_raw as (
    select
        `after`.shipment_id as shipment_id
        , `after`.order_id as order_id
        , `after`.driver_id as driver_id
        , `after`.current_location as current_location
        , parseDateTimeBestEffort(`after`.estimated_delivery_time) as estimated_delivery_time
        , `after`.`status` as `status`
        , toDateTime(`source`.ts_ms / 1000, 'Asia/Ho_Chi_Minh') as event_datetime
        , op
    from
        raw_data
)

, filtered_raw as (
    select
        *
    from
        transformed_raw
    where
        -- toDate(event_datetime) = today()
        -- toDate(event_datetime) = yesterday()
        toDate(event_datetime) = date_sub(today(), interval 2 days)  
)

select * from filtered_raw