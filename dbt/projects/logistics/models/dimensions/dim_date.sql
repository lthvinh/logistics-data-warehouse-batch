{{
    config(
        materialized = 'incremental'
        , incremental_strategy = 'append'
        , engine = 'MergeTree()'
        , unique_key = '(date_key)'
        , order_by = '(date_key)'
    )
}}

with date_generator as (
    {{ 
        dbt_utils.date_spine(
            datepart = 'day'
            , start_date = "cast('2024-01-01' as date)"
            , end_date = "cast('2026-01-01' as date)"
        )
    }}
)
, additional_info as (
    select 
        toYYYYMMDD(date_day) as date_key
        , date_day as `date`
        , toYear(date_day) as `year`
        , toQuarter(date_day) as `quarter`
        , toMonth(date_day) as `month`
        , toDayOfMonth(date_day) as `day`
        , toDayOfWeek(date_day) as day_of_week
        , toDayOfYear(date_day) as day_of_year
        , toWeek(date_day, 3) as week_of_year
        , if(toDayOfWeek(date_day) >= 6, true, false) as is_weekend
        , formatDateTime(date_day, '%W') as week_name
        , monthName(date_day) as month_name
        , concat('Q', toQuarter(date_day)) as quarter_name
    from 
        date_generator
)

select * from additional_info

{% set existing_table = load_cached_relation(this) %}
{% if existing_table %} limit 0 {% endif %}