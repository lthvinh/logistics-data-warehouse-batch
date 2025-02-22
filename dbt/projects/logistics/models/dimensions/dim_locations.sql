{{ 
    config(
        materialized ='incremental'
        , engine = 'MergeTree()'
        , order_by = '(location)'
        , unique_key = 'location'
        , incremental_strategy = 'append'
    ) 
}}

with src as (
    select * from {{ ref('stg_mysql__orders') }}
)
, pickup_address as (
    select pickup_address as location from src
)
, delivery_address as (
    select delivery_address as location from src
)
, union_table as (
    select location from pickup_address
    union distinct
    select location from delivery_address
     
)

{% if not is_incremental() %}
, dim_locations as (
    select
        row_number() over(order by `location`)  as location_key
        , location
    from
        union_table    
)
{% else %}

{% set sql_statement %}
    select max(location_key) from {{ this }}
{% endset %}

{% set location_key_max = dbt_utils.get_single_value(sql_statement) %}

, dim_locations as (
    select
        row_number() over(order by s.`location`) + {{ location_key_max }}  as location_key
        , s.location
    from
        union_table as s
    left anti join
        ( select * from {{this}} ) as t on s.location = t.location
)
{% endif %}

select * from dim_locations
