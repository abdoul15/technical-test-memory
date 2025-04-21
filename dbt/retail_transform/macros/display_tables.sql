{% macro display_final_tables() %}

    {% set tables = ['dim_clients', 'dim_products', 'dim_stores', 'fct_transactions'] %}
    
    {% for table in tables %}
        {% set query %}
            SELECT * FROM {{ ref(table) }} LIMIT 10;
        {% endset %}
        
        {% set results = run_query(query) %}
        
        {% do log('', info=True) %}
        {% do log('=== ' ~ table ~ ' (10 premières lignes) ===', info=True) %}
        {% do log('', info=True) %}
        {% do results.print_table() %}
        {% do log('', info=True) %}
        {% do log('======================================', info=True) %}
        {% do log('', info=True) %}
    {% endfor %}

{% endmacro %}
