with raw_data as (
    select
        *
    from
        {{ source('raw', 'logistics_src.logistics.Drivers') }}
)

, transformed_raw as (
    select
        `after`.driver_id as driver_id
        , `after`.user_id as user_id
        , `after`.vehicle_license_plate as vehicle_license_plate
        , `after`.vehicle_type  as vehicle_type
        , `after`.vehicle_year as vehicle_year
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
        toDate(event_datetime) = yesterday()
)

select * from filtered_raw