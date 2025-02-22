{% macro create_memory_table(temp_relation, sql) %}

    create table {{temp_relation}}
    engine = Memory
    as
    {{sql}};

{% endmacro%}