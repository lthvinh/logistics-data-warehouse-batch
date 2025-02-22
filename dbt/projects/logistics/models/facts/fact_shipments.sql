{{
    config(
        materialized = 'incremental'
        , incremental_strategy = 'append'
        , engine = 'ReplacingMergeTree'
        , unique_key = '(shipment_id, order_id)'
        , order_by = '(shipment_id)'
        , post_hook = 'optimize table {{ this }}'
    )
}}

with src_data as (
 	select * from {{ ref('stg_mysql__shipments') }} 
)
, fact_orders as (
    select 
        order_id
        , user_key
        , pick_up_location_key
        , delivery_location_key
        , package_description
        , created_order_date_key
        , created_order_time_key
        , package_weight
    from
  	    {{ ref('fact_orders') }}
)
, dim_drivers as (
 	select
        driver_key
        , driver_id
 	from
        {{ ref('dim_drivers') }}
 )
, joined_shipments as (
	select
        s.shipment_id as shipment_id
        , s.order_id as order_id
        , d.driver_key as driver_key
        , s.current_location as dd_current_location
        , o.user_key as user_key
        , o.pick_up_location_key as pick_up_location_key
        , o.delivery_location_key as delivery_location_key
        , o.package_description as dd_package_description
        , o.created_order_date_key as created_order_date_key
        , o.created_order_time_key as created_order_time_key
        , s.`status` as dd_status
        , toYYYYMMDD(s.estimated_delivery_time) as estimated_delivery_date_key
        , formatDateTime(s.estimated_delivery_time, '%T') as estimated_delivery_time_key
 	from
  	    src_data as s
    left join
  	    fact_orders as o on s.order_id = o.order_id
    left join
  	    dim_drivers as d on s.driver_id = d.driver_id
)

select * from joined_shipments