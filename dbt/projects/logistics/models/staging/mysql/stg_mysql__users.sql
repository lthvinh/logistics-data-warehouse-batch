with raw_data as (
    select
        *
    from
        {{ source('raw', 'logistics_src.logistics.Users') }}
)

, transformed_raw as (
    select
        `after`.user_id as user_id
        , `after`.full_name as full_name
        , `after`.email as email
        , `after`.phone_number as phone_number
        , `after`.`address` as `address`
        , `after`.`role` as `role`
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