with raw_data as (
    select
        *
    from
        {{ source('raw', 'logistics_src.logistics.Orders') }}
)

, transformed_raw as (
    select
        `after`.order_id as order_id
        , `after`.user_id as user_id
        , `after`.pickup_address as pickup_address
        , `after`.delivery_address as delivery_address
        , `after`.package_description as package_description
        , `after`.package_weight as package_weight
        , parseDateTimeBestEffort(`after`.delivery_time) as delivery_time
        , `after`.status as `status`
        , parseDateTimeBestEffort(`after`.created_at) as created_at
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




