{{
    config(
        materialized = 'scd_type_2'
        , unique_key = 'user_key'
        , unique_id = 'user_id'
    )
}}

{% set columns = ['user_id', 'full_name', 'email', 'phone_number', 'address', 'role', 'created_at'] %}

{% set existing_table = load_cached_relation(this) %}

{% if existing_table is none %}

    select
        user_id
        , full_name
        , email
        , phone_number
        , `address`
        , `role`
        , created_at
        , {{ dbt_utils.generate_surrogate_key(columns)}} as hash
        , event_datetime
    from
        {{ ref('stg_mysql__users') }}

{% else %}
    
    select
        s.* 
    from 
    (
        select
            user_id
            , full_name
            , email
            , phone_number
            , `address`
            , `role`
            , created_at
            , {{ dbt_utils.generate_surrogate_key(columns)}} as hash
            , event_datetime
        from 
            {{ ref('stg_mysql__users') }} 
    ) as s
    left any join
        (select * from {{ this }} where is_current) as t on s.user_id = t.user_id
    where
        (s.event_datetime > t.effective_from_datetime and s.hash != t.hash) or (t.user_id is null)
    settings join_use_nulls = 1

{% endif%}