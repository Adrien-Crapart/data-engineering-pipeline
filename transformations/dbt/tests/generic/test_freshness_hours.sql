{% test freshness_hours(model, column_name, max_hours=24) %}
{#
    Verify that the most recent record is not older than max_hours.
    Useful for data freshness SLA checks.
    Usage in schema.yml:
      - freshness_hours:
          column_name: ingested_at
          max_hours: 12
#}

select max({{ column_name }}) as most_recent
from {{ model }}
having
    max({{ column_name }}) < current_timestamp - interval '{{ max_hours }} hours'
    or max({{ column_name }}) is null

{% endtest %}
