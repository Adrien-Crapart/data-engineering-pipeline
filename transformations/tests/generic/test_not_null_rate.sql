{% test not_null_rate(model, column_name, max_null_rate=0.05) %}
{#
    Verify that the null rate for a column does not exceed the threshold.
    Default threshold: 5% nulls allowed.
    Usage in schema.yml:
      - not_null_rate:
          max_null_rate: 0.02
#}

select count(*) as failures
from {{ model }}
where {{ column_name }} is null
having
    cast(count(*) as float) / nullif(
        (select count(*) from {{ model }}), 0
    ) > {{ max_null_rate }}

{% endtest %}
