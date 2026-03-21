{% test value_range(model, column_name, min_value=None, max_value=None) %}
{#
    Verify that all values in a column fall within the expected range.
    Usage in schema.yml:
      - value_range:
          min_value: -90
          max_value: 90
#}

select count(*) as failures
from {{ model }}
where {{ column_name }} is not null
    {% if min_value is not none %}
    and {{ column_name }} < {{ min_value }}
    {% endif %}
    {% if max_value is not none %}
    and {{ column_name }} > {{ max_value }}
    {% endif %}
having count(*) > 0

{% endtest %}
