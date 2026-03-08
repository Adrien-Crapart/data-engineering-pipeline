"""Data contract validator for incoming API responses.

Validates raw JSON data against YAML contract definitions to enforce
schema governance before storage and transformation.
"""

from __future__ import annotations

import logging
import time
from pathlib import Path
from typing import Any

import yaml

logger = logging.getLogger(__name__)

CONTRACTS_DIR = Path(__file__).parent

_TYPE_MAP = {
    "string": str,
    "integer": int,
    "number": (int, float),
    "boolean": bool,
    "array": list,
    "object": dict,
}


class ContractViolation(Exception):
    """Raised when data does not conform to its contract."""


def _load_contract(contract_path: Path) -> dict:
    with open(contract_path) as f:
        return yaml.safe_load(f)["contract"]


def _validate_field(data: dict, field_spec: dict, path: str = "") -> list[str]:
    """Validate a single field against its spec, returning a list of errors."""
    errors: list[str] = []
    name = field_spec["name"]
    full_path = f"{path}.{name}" if path else name
    required = field_spec.get("required", False)

    if name not in data:
        if required:
            errors.append(f"Missing required field: {full_path}")
        return errors

    value = data[name]
    expected_type = field_spec.get("type")

    if expected_type and expected_type in _TYPE_MAP:
        python_type = _TYPE_MAP[expected_type]
        if not isinstance(value, python_type):
            errors.append(
                f"Type mismatch at {full_path}: expected {expected_type}, "
                f"got {type(value).__name__}"
            )

    if expected_type == "object" and "fields" in field_spec and isinstance(value, dict):
        for sub_field in field_spec["fields"]:
            errors.extend(_validate_field(value, sub_field, full_path))

    return errors


def validate_contract(data: dict[str, Any], contract_name: str) -> list[str]:
    """Validate data against a named contract.

    Returns a list of violation messages (empty if valid).
    """
    contract_path = CONTRACTS_DIR / f"{contract_name}_contract.yaml"
    if not contract_path.exists():
        raise FileNotFoundError(f"Contract not found: {contract_path}")

    contract = _load_contract(contract_path)
    errors: list[str] = []

    for field_spec in contract["schema"]["fields"]:
        errors.extend(_validate_field(data, field_spec))

    if errors:
        logger.warning(
            "Contract %s v%s: %d violation(s) found",
            contract["name"],
            contract["version"],
            len(errors),
        )
        for err in errors:
            logger.warning("  - %s", err)
    else:
        logger.info("Contract %s v%s: validation passed", contract["name"], contract["version"])

    return errors


def validate_freshness(data: dict[str, Any], contract_name: str) -> bool:
    """Check that the data timestamp is within the freshness window."""
    contract_path = CONTRACTS_DIR / f"{contract_name}_contract.yaml"
    if not contract_path.exists():
        raise FileNotFoundError(f"Contract not found: {contract_path}")

    contract = _load_contract(contract_path)
    max_age = contract.get("freshness", {}).get("max_age_minutes")
    if max_age is None:
        return True

    dt_field = data.get("dt")
    if dt_field is None:
        logger.warning("No 'dt' field found for freshness check")
        return True

    age_minutes = (time.time() - dt_field) / 60
    is_fresh = age_minutes <= max_age
    if not is_fresh:
        logger.warning(
            "Freshness violation: data is %.1f minutes old (max: %d)",
            age_minutes,
            max_age,
        )
    return is_fresh
