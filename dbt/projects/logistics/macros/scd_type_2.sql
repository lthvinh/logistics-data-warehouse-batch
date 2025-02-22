{% materialization scd_type_2, adapter = 'default' %}

    {% set unique_key = config.require('unique_key') %}
    {% set unique_id = config.require('unique_id') %}
    {% set order_by = config.get('order_by') %}

    {% set target_relation = this %}
    {% set temp_relation = make_temp_relation( target_relation ) %}

    {% call statement('drop_temp_table') %}
        {{ drop_table(temp_relation) }}
    {% endcall %}

    {{ run_hooks(pre_hooks) }}

    {% set existing = load_cached_relation(target_relation) %}

    {% if existing is none or should_full_refresh() %}

        {% call statement('drop_target_table') %}

            {{ drop_table(target_relation) }}

        {% endcall %}

            {% set temp_relation_query %}

                select 
                    row_number() over(order by {{ unique_id }} ) as {{ unique_key }}
                    , *
                    , event_datetime as effective_from_datetime
                    , leadInFrame(event_datetime, 1, makeDateTime(2100, 1, 1, 0, 0, 0))
                        over(partition by {{ unique_id }} order by {{ unique_id }}, event_datetime
                        rows between unbounded preceding and unbounded following
                    ) as effective_to_datetime
                    , if(effective_to_datetime = makeDateTime(2100, 1, 1, 0, 0, 0), true, false ) as is_current
                from
                    ({{ sql }})

            {% endset %}

        {% call statement('create_temp_table') %}

            {{ create_memory_table( temp_relation, temp_relation_query ) }}

        {% endcall %}


        {% set build_sql %}

            create table {{ target_relation }}
            engine = MergeTree()
            primary key ( {{ unique_id }} )
            {% if order_by is not none %} order by = {{ order_by }} {% endif %}
            as
            select * from {{ temp_relation }}

        {% endset %}

        {% call statement('main') %}

            {{ build_sql }}

        {% endcall%}

    {% else %}

        {% set upsert_relation = target_relation.derivative('__snapshot_upsert') %}

        {% call statement('create_upsert_relation') %}

            create table if not exists {{ upsert_relation }} as {{ target_relation }}
            
        {% endcall %}

        {% set unique_key_max_query %}

            select max({{ unique_key }}) from {{ target_relation }}

        {% endset %}

        {% set unique_key_max = dbt_utils.get_single_value(unique_key_max_query, default = 0 ) %}

        {% set temp_relation_query %}

            select 
                row_number() over(order by {{ unique_id }} ) + {{ unique_key_max }} as {{ unique_key }}
                , *
                , event_datetime as effective_from_datetime
                , leadInFrame(event_datetime, 1, makeDateTime(2100, 1, 1, 0, 0, 0))
                    over(partition by {{ unique_id }} order by {{ unique_id }}, event_datetime
                    rows between unbounded preceding and unbounded following
                ) as effective_to_datetime
                , if(effective_to_datetime = makeDateTime(2100, 1, 1, 0, 0, 0), true, false ) as is_current
            from
                ( {{ sql }} )

        {% endset %}

        {% call statement('create_temp_table') %}

            {{ create_memory_table( temp_relation, temp_relation_query ) }}

        {% endcall %}
        
        {% set insert_cols = dbt_utils.get_filtered_columns_in_relation(
            from = target_relation
            ,  except = ['effective_to_datetime', 'is_current']
        ) %}

        {% set existing_data_query %}

            select
                {% for column in insert_cols %}

                {% if not loop.first %} , {% endif %} t.{{ column }}

                {% endfor %}

                , if( s.{{ unique_id }} is not null, s.effective_from_datetime, t.effective_to_datetime) as effective_to_datetime
                , if( s.{{ unique_id }} is not null, false, t.is_current) as is_current
            from
                {{ target_relation }} as t
            left any join
                (
                    select {{ unique_id }}, min(effective_from_datetime) as effective_from_datetime 
                    from {{ temp_relation }} 
                    group by {{ unique_id}} 
                ) as s
                on s.{{unique_id}} = t.{{ unique_id }} and t.is_current
            settings join_use_nulls = 1;

        {% endset %}

        {% call statement('insert_existing_data') %}

            insert into {{ upsert_relation }}  ( {{ insert_cols | join(', ') }},  effective_to_datetime, is_current )
            {{ existing_data_query }}

        {% endcall %}

        {% call statement('main') %}

            insert into {{ upsert_relation }} ( {{ insert_cols | join(', ') }},  effective_to_datetime, is_current )
            select {{ insert_cols | join(', ') }},  effective_to_datetime, is_current  
            from {{ temp_relation }}

        {% endcall %}
        
        {% call statement('drop_target_relation') %}

            {{ drop_table(target_relation) }}

        {% endcall %}

        {% call statement('rename_upsert_relation') %}

            rename table {{ upsert_relation }} to {{ target_relation}}
            
        {% endcall %}

    {% endif %}

    
    {% call statement('drop_temp_table') %}

        {{ drop_table(temp_relation) }}

    {% endcall %}

    {{ run_hooks(post_hooks)}}

    {{ adapter.commit() }}

    {{ return({"relations": [target_relation]}) }}

{% endmaterialization %}



