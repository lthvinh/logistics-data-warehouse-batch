-- depends_on: {{ ref('stg_mysql__users') }}

{{
    config(
        materialized = 'scd_type_2'
        , unique_key = 'driver_key'
        , unique_id = 'driver_id'
    )
}}
{% set columns = ['driver_id', 'user_id', 'full_name', 'email', 'phone_number', 'address', 'vehicle_license_plate', 'vehicle_type', 'vehicle_year'] %}

{% set existing_table = load_cached_relation(this) %}

{% if existing_table is none %}

    select
        d.driver_id
        , d.user_id
        , u.full_name
        , u.email
        , u.phone_number
        , u.address
        , d.vehicle_license_plate
        , d.vehicle_type
        , d.vehicle_year
        , {{ dbt_utils.generate_surrogate_key(columns) }} as hash
        , d.event_datetime
    from 
        {{ref('stg_mysql__drivers')}} as d
    join 
        ( select * from {{ ref('dim_users') }} where is_current) as u
        on d.user_id = u.user_id

{% else %}

    with dim_drivers as (
        select * from {{ this }} where is_current
    )
    , dim_users as (
        select * from {{ ref('dim_users') }} where is_current
    )
    , users_source as (
        select user_id, max(event_datetime) as event_datetime from {{ ref('stg_mysql__users') }}
        where op = 'u' and `role` = 'driver'
        group by user_id
    )
    , drivers_source as (
        select 
            driver_id
            , user_id
            , vehicle_license_plate
            , vehicle_type
            , vehicle_year
            , event_datetime
        from 
            {{ ref('stg_mysql__drivers') }}
    )
    , update_users as (
        select
            t.driver_id
            , t.user_id
            , t.vehicle_license_plate
            , t.vehicle_type
            , t.vehicle_year
            , s.event_datetime
        from
            dim_drivers as t
        left semi join
            (
                select u.user_id, u.event_datetime from users_source as u
                left anti join drivers_source  as d on u.user_id = d.user_id
            ) as s on t.user_id = s.user_id
        settings join_use_nulls = 1     
    )
    , update_union as (
        select * from drivers_source
        union distinct
        select * from update_users
    )
    , final as (
        select
            d.driver_id
            , d.user_id
            , u.full_name
            , u.email
            , u.phone_number
            , u.address
            , d.vehicle_license_plate
            , d.vehicle_type
            , d.vehicle_year
            , d.event_datetime
        from 
            update_union as d
        join 
            dim_users as u
            on d.user_id = u.user_id
        settings join_use_nulls = 1
    )
    , filtered_final as (
        select 
            s.* 
        from 
        (
            select
                driver_id
                , user_id
                , full_name
                , email
                , phone_number
                , address
                , vehicle_license_plate
                , vehicle_type
                , vehicle_year
                , {{ dbt_utils.generate_surrogate_key(columns) }} as hash
                , event_datetime
            from
                final
        ) as s
        left any join
            dim_drivers as t on s.driver_id = t.driver_id
        where 
            (s.event_datetime > t.effective_from_datetime and s.hash != t.hash) or (t.driver_id is null)
        settings join_use_nulls = 1
    )
    select * from filtered_final

{% endif %}