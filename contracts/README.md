# Contracts

Data contract definitions and validation logic for enforcing schema governance
on incoming API data.

## Purpose

Data contracts define the expected schema, data types, required fields, nullability
constraints, and freshness expectations for each data source. The ingestion pipeline
validates incoming data against these contracts before storage or transformation.

## Key Files

| File | Description |
|------|-------------|
| `weather_current_contract.yaml` | Schema contract for current weather API responses |
| `weather_forecast_contract.yaml` | Schema contract for forecast API responses |
| `validator.py` | Python module that validates a dict against a YAML contract |

## Contract Format

```yaml
contract:
  name: weather_current
  version: "1.0"
  owner: data-engineering
  freshness:
    max_age_minutes: 360
  schema:
    fields:
      - name: main
        type: object
        required: true
```

## Validation

```python
from contracts.validator import validate_contract

errors = validate_contract(data, "weather_current")
# Returns [] if valid, or a list of violation messages
```
