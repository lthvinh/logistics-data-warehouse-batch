{{
    config(
        materialized = 'incremental'
        , incremental_strategy = 'append'
        , engine = 'ReplacingMergeTree'
        , unique_key = '(payment_id, order_id)'
        , order_by = '(payment_id)'
        , post_hook = 'optimize table {{ this }}'
    )
}}

with src_data as (
	select 
        *
    from 
        {{ ref('stg_mysql__payments') }} 
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
, joined_payments as (
	select
        p.payment_id as payment_id
        , p.order_id as order_id
        , o.user_key as user_key
        , o.pick_up_location_key as pick_up_location_key
        , o.delivery_location_key as delivery_location_key
        , o.package_description as dd_package_description
        , o.created_order_date_key as created_order_date_key
        , o.created_order_time_key as created_order_time_key
        , toYYYYMMDD(p.payment_date) as payment_date_key
        , formatDateTime(p.payment_date, '%T') as payment_time_key
        , p.payment_method as dd_payment_method
        , p.payment_status as dd_payment_status
        , o.package_weight as package_weight
        , p.amount as amount
 	from
  	    src_data as p
    left join
  	    fact_orders as o on p.order_id = o.order_id
)

select * from joined_payments






