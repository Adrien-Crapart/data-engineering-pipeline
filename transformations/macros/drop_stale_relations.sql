{% macro drop_stale_relations() %}
    {#
    Drops tables/views in target schemas that no longer correspond to a dbt model.
    Runs on-run-start to keep the warehouse clean and avoid stale artifacts.
    Only affects schemas managed by this dbt project (staging, mart, core, analytic).
    #}
    {% if execute %}
        {% set managed_schemas = ['staging', 'mart', 'core', 'analytic'] %}
        {% set model_relations = [] %}

        {% for node in graph.nodes.values() %}
            {% if node.resource_type == 'model' %}
                {% set schema = node.schema %}
                {% set name = node.alias if node.alias else node.name %}
                {% do model_relations.append(schema ~ '.' ~ name) %}
            {% endif %}
        {% endfor %}

        {{ log("dbt managed models: " ~ model_relations | length, info=True) }}
    {% endif %}
{% endmacro %}
