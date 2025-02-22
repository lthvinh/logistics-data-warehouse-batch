{% macro generate_hash(relation, except=[], prefix = '') %}

    {% set cols = dbt_utils.get_filtered_columns_in_relation(from = relation, except = except) %}
    {% set temp_cols = [] %}

    {% for col in cols %}

        {% do temp_cols.append('ifNull(toString(' ~ prefix ~ col ~ "), ' ')") %}

    {% endfor %}
    SHA256(concat_ws('|', {{ temp_cols | join(', ') }} ))
{% endmacro %}
