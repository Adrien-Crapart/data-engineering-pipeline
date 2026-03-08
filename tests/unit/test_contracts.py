"""Tests for the data contract validator."""

from __future__ import annotations

import time
from unittest.mock import patch

import pytest

from contracts.validator import (
    _validate_field,
    validate_contract,
    validate_freshness,
)


class TestValidateField:
    def test_valid_required_string(self):
        data = {"name": "Paris"}
        spec = {"name": "name", "type": "string", "required": True}
        assert _validate_field(data, spec) == []

    def test_missing_required_field(self):
        data = {}
        spec = {"name": "name", "type": "string", "required": True}
        errors = _validate_field(data, spec)
        assert len(errors) == 1
        assert "Missing required field" in errors[0]

    def test_missing_optional_field(self):
        data = {}
        spec = {"name": "name", "type": "string", "required": False}
        assert _validate_field(data, spec) == []

    def test_type_mismatch(self):
        data = {"temp": "not_a_number"}
        spec = {"name": "temp", "type": "number", "required": True}
        errors = _validate_field(data, spec)
        assert len(errors) == 1
        assert "Type mismatch" in errors[0]

    def test_number_accepts_int_and_float(self):
        for value in [42, 3.14]:
            data = {"temp": value}
            spec = {"name": "temp", "type": "number", "required": True}
            assert _validate_field(data, spec) == []

    def test_nested_object_validation(self):
        data = {"main": {"temp": 20.5, "humidity": 65}}
        spec = {
            "name": "main",
            "type": "object",
            "required": True,
            "fields": [
                {"name": "temp", "type": "number", "required": True},
                {"name": "humidity", "type": "integer", "required": True},
            ],
        }
        assert _validate_field(data, spec) == []

    def test_nested_object_missing_child(self):
        data = {"main": {"temp": 20.5}}
        spec = {
            "name": "main",
            "type": "object",
            "required": True,
            "fields": [
                {"name": "temp", "type": "number", "required": True},
                {"name": "humidity", "type": "integer", "required": True},
            ],
        }
        errors = _validate_field(data, spec)
        assert len(errors) == 1
        assert "main.humidity" in errors[0]

    def test_array_type(self):
        data = {"items": [1, 2, 3]}
        spec = {"name": "items", "type": "array", "required": True}
        assert _validate_field(data, spec) == []

    def test_boolean_type(self):
        data = {"active": True}
        spec = {"name": "active", "type": "boolean", "required": True}
        assert _validate_field(data, spec) == []


class TestValidateContract:
    def test_valid_current_weather(self):
        data = {
            "main": {"temp": 20.5, "humidity": 65, "pressure": 1013},
            "wind": {"speed": 5.2},
            "weather": [{"main": "Clear"}],
            "name": "Paris",
            "dt": int(time.time()),
            "coord": {"lon": 2.35, "lat": 48.85},
        }
        errors = validate_contract(data, "weather_current")
        assert errors == []

    def test_missing_required_fields(self):
        data = {"name": "Paris"}
        errors = validate_contract(data, "weather_current")
        assert len(errors) > 0
        field_names = " ".join(errors)
        assert "main" in field_names

    def test_contract_not_found(self):
        with pytest.raises(FileNotFoundError):
            validate_contract({}, "nonexistent_contract")

    def test_valid_forecast(self):
        data = {
            "list": [{"dt": 1234567890}],
            "city": {"name": "Paris", "coord": {"lon": 2.35, "lat": 48.85}},
            "cnt": 40,
        }
        errors = validate_contract(data, "weather_forecast")
        assert errors == []


class TestValidateFreshness:
    def test_fresh_data(self):
        data = {"dt": time.time() - 60}
        assert validate_freshness(data, "weather_current") is True

    def test_stale_data(self):
        data = {"dt": time.time() - 999999}
        assert validate_freshness(data, "weather_current") is False

    def test_no_dt_field(self):
        data = {"name": "Paris"}
        assert validate_freshness(data, "weather_current") is True

    def test_contract_not_found(self):
        with pytest.raises(FileNotFoundError):
            validate_freshness({}, "nonexistent")
