with raw_data as (
    select
        *
    from
        {{ source('raw', 'logistics_src.logistics.Payments') }}
)

, transformed_raw as (
    select
        `after`.payment_id as payment_id
        , `after`.order_id as order_id
        , `after`.amount as amount
        , `after`.payment_method as payment_method
        , `after`.payment_status as payment_status
        , parseDateTimeBestEffort(`after`.payment_date) as payment_date
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