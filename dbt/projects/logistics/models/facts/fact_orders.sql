{{
    config(
        materialized = 'incremental'
        , incremental_strategy = 'append'
        , engine = 'ReplacingMergeTree'
        , unique_key = '(order_id)'
        , order_by = '(order_id)'
        , pre_hook = [
            """
                create function if not exists get_interval_map as (n) -> 
                    map(
                    'days', cast(intDiv(n, 86400) as UInt8)
                    , 'hours', cast(intDiv(mod(n, 86400), 3600) as UInt8)
                    , 'minutes', cast(intDiv(mod(n, 3600), 60) as UInt8)
                    , 'seconds', cast(mod(n, 60) as UInt8) 
                );
            """
            ,
            """
                create function if not exists create_datetime as (date_key, time_key) ->
                    parseDateTimeBestEffort(concat(date_key, ' ', time_key)) ;
            """
        ]
        , post_hook = [
            'drop table if exists temp_fact_orders;'
            , 'optimize table {{ this }};'
            , 'drop function if exists get_interval_map;'
            , 'drop function if exists create_datetime;'
            ]
        , settings = {'allow_nullable_key': 1} 
    )
}}


with orders_source as (
    select 
        order_id
        , user_id
        , pickup_address
        , delivery_address
        , package_description
        , package_weight
        , delivery_time
        , `status`
        , created_at
        , event_datetime
        , min(event_datetime) over (partition by order_id, `status`) as min_event_datetime
        , row_number() over(partition by order_id, `status` order by event_datetime desc) as rn
    from 
        {{ ref('stg_mysql__orders') }}
)
, with_row_number as (
    select 
        order_id
        , user_id
        , pickup_address
        , delivery_address
        , package_description
        , package_weight
        , delivery_time
        , `status`
        , created_at
        , min_event_datetime as event_datetime
    from 
  		orders_source
    where
  		rn = 1
)
, joined_orders as (
	select
        o.order_id
        , u.user_key
        , pk.location_key as pick_up_location_key
        , d.location_key as delivery_location_key
        , o.package_description
        , o.package_weight
        , o.delivery_time
        , o.status
        , o.created_at
        , o.event_datetime
    from
  		with_row_number as o
    left join
  		(select user_key, user_id from {{ ref('dim_users')}}) as u on o.user_id = u.user_id
    left join
  		{{ ref('dim_locations') }} as pk on o.pickup_address = pk.location
 	left join
  		{{ ref('dim_locations') }} as d on o.delivery_address = d.location
    settings join_use_nulls = 1
)

{% if is_incremental() %}

, fact_orders as (
	select * from {{ this }}
)

{% else %}

{% call statement(creating_temp_table) %}
    create table if not exists temp_fact_orders(
        order_id UInt32
        , user_key UInt64
        , pick_up_location_key UInt64
        , delivery_location_key UInt64
        , package_description String
        , created_order_date_key UInt32
        , created_order_time_key String
        , accepted_date_key UInt32
        , accepted_time_key String
        , in_transit_date_key UInt32
        , in_transit_time_key String
        , delivered_date_key UInt32
        , delivered_time_key String
        , delivery_date_key UInt32
        , delivery_time_key String
        , package_weight Float32
        , created_to_accepted_lag Map(String, UInt8)
        , accepted_to_in_transit_lag Map(String, UInt8)
        , in_transit_to_delivered_lag Map(String, UInt8)
        , delivered_and_delivery_difference Map(String, UInt8)
        , delivered_is_greater_than_delivery bool
        , status String
    )
    engine = Memory;

{% endcall %}

, fact_orders as (
	select * from temp_fact_orders
)

{% endif %}

, processing_orders as (
	select * from joined_orders
  	where status = 'processing'
)
, accepted_orders as (
	select * from joined_orders
  	where status = 'accepted'
)
, in_transit_orders as (
	select * from joined_orders
  	where status = 'in_transit'
)  
, delivered_orders as (
	select * from joined_orders
  	where status = 'delivered'
) 
, update_data as (
    select
        coalesce(d.order_id, it.order_id, a.order_id, p.order_id) as order_id
        , coalesce(d.user_key, it.user_key, a.user_key, p.user_key) as user_key
        , coalesce(d.pick_up_location_key, it.pick_up_location_key, a.pick_up_location_key, p.pick_up_location_key) as pick_up_location_key
        , coalesce(d.delivery_location_key, it.delivery_location_key, a.delivery_location_key, p.delivery_location_key) as delivery_location_key
        , coalesce(d.package_description, it.package_description, a.package_description, p.package_description) as package_description
        , toYYYYMMDD(coalesce(d.created_at, it.created_at, a.created_at, p.created_at)) as created_order_date_key	
        , formatDateTime(coalesce(d.created_at, it.created_at, a.created_at, p.created_at), '%T') as created_order_time_key
        , case
            when isNull(a.event_datetime) and isNull(o.accepted_date_key) then 21000101
            when isNull(a.event_datetime) or isNull(o.accepted_date_key) then coalesce(toYYYYMMDD(a.event_datetime), o.accepted_date_key)
            when o.accepted_date_key = 21000101 then toYYYYMMDD(a.event_datetime)
            else o.accepted_date_key
        end as accepted_date_key
        , case
            when isNull(a.event_datetime) and isNull(o.accepted_time_key) then '00:00:00'
            when isNull(a.event_datetime) or isNull(o.accepted_time_key) then coalesce(formatDateTime(a.event_datetime, '%T'), o.accepted_time_key)
            when o.accepted_time_key = 21000101 then formatDateTime(a.event_datetime, '%T')
            else o.accepted_time_key
        end as accepted_time_key
        , case
            when isNull(it.event_datetime) and isNull(o.in_transit_date_key) then 21000101
            when isNull(it.event_datetime) or isNull(o.in_transit_date_key) then coalesce(toYYYYMMDD(it.event_datetime), o.in_transit_date_key)
            when o.in_transit_date_key = 21000101 then toYYYYMMDD(it.event_datetime)
            else o.in_transit_date_key
        end as in_transit_date_key
        , case
            when isNull(it.event_datetime) and isNull(o.in_transit_time_key) then '00:00:00'
            when isNull(it.event_datetime) or isNull(o.in_transit_time_key) then coalesce(formatDateTime(it.event_datetime, '%T'), o.in_transit_time_key)
            when o.in_transit_time_key = 21000101 then formatDateTime(it.event_datetime, '%T')
            else o.in_transit_time_key
        end as in_transit_time_key
        , case
            when isNull(d.event_datetime) and isNull(o.delivered_date_key) then 21000101
            when isNull(d.event_datetime) or isNull(o.delivered_date_key) then coalesce(toYYYYMMDD(d.event_datetime), o.delivered_date_key)
            when o.delivered_date_key = 21000101 then toYYYYMMDD(d.event_datetime)
            else o.delivered_date_key
        end as delivered_date_key
        , case
            when isNull(d.event_datetime) and isNull(o.delivered_time_key) then '00:00:00'
            when isNull(d.event_datetime) or isNull(o.delivered_time_key) then coalesce(formatDateTime(d.event_datetime, '%T'), o.delivered_time_key)
            when o.delivered_time_key = 21000101 then formatDateTime(d.event_datetime, '%T')
            else o.delivered_time_key
        end as delivered_time_key
        , toYYYYMMDD(coalesce(d.delivery_time, it.delivery_time, a.delivery_time, p.delivery_time)) as delivery_date_key	
        , formatDateTime(coalesce(d.delivery_time, it.delivery_time, a.delivery_time, p.delivery_time), '%T') as delivery_time_key
        , coalesce(d.package_weight, it.package_weight, a.package_weight, p.package_weight) as package_weight
        , if(
            accepted_date_key != 21000101
            , get_interval_map(create_datetime(accepted_date_key, accepted_time_key) - create_datetime(created_order_date_key, created_order_time_key))
            , map('days', 0, 'hours', 0, 'minutes', 0, 'seconds', 0)
        ) as created_to_accepted_lag
        , if(
            in_transit_date_key != 21000101
            , get_interval_map(create_datetime(in_transit_date_key, in_transit_time_key) - create_datetime(accepted_date_key, accepted_time_key))
            , map('days', 0, 'hours', 0, 'minutes', 0, 'seconds', 0)
        ) as accepted_to_in_transit_lag
        , if(
            delivered_date_key != 21000101
            , get_interval_map(create_datetime(delivered_date_key, delivered_time_key) - create_datetime(in_transit_date_key, in_transit_time_key))
            , map('days', 0, 'hours', 0, 'minutes', 0, 'seconds', 0)
        ) as in_transit_to_delivered_lag
        , if(
            delivered_date_key != 21000101
            , get_interval_map(abs(create_datetime(delivery_date_key, delivery_time_key) - create_datetime(delivered_date_key, delivered_time_key)))
            , map('days', 0, 'hours', 0, 'minutes', 0, 'seconds', 0)
        ) as delivered_and_delivery_difference
  			, if(
          	create_datetime(delivered_date_key, delivered_time_key) > create_datetime(delivery_date_key, delivery_time_key)
        		, true
          	, false
        ) as delivered_is_greater_than_delivery          	     		
        , coalesce(d.status, it.status, a.status, p.status) as dd_status
  	from
        processing_orders as p
  	full outer join
        accepted_orders as a on p.order_id = a.order_id
    full outer join
        in_transit_orders as it on a.order_id = it.order_id
    full outer join
        delivered_orders as d on it.order_id = d.order_id
  	left join
        fact_orders as o on coalesce(p.order_id, a.order_id, it.order_id, d.order_id) = o.order_id
  	settings join_use_nulls = 1
)

select * from update_data