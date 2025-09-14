import html
from datetime import datetime
from typing import Dict, Any, Tuple, List
import re
from .location import standardize_locations
# Map string type names to Python types for casting
TYPE_MAP = {
    "str": str, "string": str, "int": int, "integer": int,
    "bigint": int, "float": float, "bool": bool, "list": list
}

def validate_datetime_string(dt_string: str) -> bool:
    """Checks if a string is a valid ISO 8601 format."""
    try:
        if isinstance(dt_string, str):
            datetime.fromisoformat(dt_string.replace('Z', '+00:00'))
            return True
    except (ValueError, TypeError):
        return False
    return False

def standardize_employment_type(value):
    """Standardizes employment_type to either 'FULL_TIME' or 'PART_TIME'."""
    if not value:
        return "PART_TIME"
    cleaned = re.sub(r'[^A-Za-z0-9]', '', str(value)).upper()
    if cleaned == "FULLTIME":
        return "FULL_TIME"
    else:
        return "PART_TIME"

def standardize_status(value):
    """Standardizes status to either 'CLOSED' or 'ACTIVE'."""
    if not value:
        return "ACTIVE"
    closed_values = ['closed', 'expired', 'filled', 'inactive', 'not available']
    value_clean = str(value).strip().lower()
    for closed in closed_values:
        if closed in value_clean:
            return "CLOSED"
    return "ACTIVE"


def transform_job_data(raw_data: Dict[str, Any], mapping_config: Dict) -> Dict[str, Any]:
    """
    Transforms raw job data by standardizing types and values, and then applies
    a multi-step fallback logic to standardize location fields.
    This function assumes mapping from source to target keys has already occurred.
    """
    final_data = {}
    target_schema = mapping_config.get("target_schema", {})

    # Pass 1: Standardize and type-convert the data based on the target schema
    for target_key, value in raw_data.items():
        if isinstance(value, str):
            value = html.unescape(value)
        if target_key == 'locations':
            continue

        if target_key in target_schema and value is not None:
            if target_key == 'status':
                final_data[target_key] = standardize_status(value)
            elif target_key == 'employment_type':
                final_data[target_key] = standardize_employment_type(value)
            elif target_key.startswith('is_'):
                final_data[target_key] = str(value).lower() in ['true', '1', 'yes']
            elif target_key.startswith('salary_') or target_key in ['cpc', 'cpa']:
                try:
                    final_data[target_key] = float(value)
                except (TypeError, ValueError):
                    final_data[target_key] = None
            else:
                final_data[target_key] = value
        else:
            final_data[target_key] = value

    city = raw_data.get('city')
    state = raw_data.get('state')
    country = raw_data.get('country')
    raw_location_value = None
    if city or state or country:
        parts = [part for part in [city, state, country] if part]
        raw_location_value = ", ".join(parts)
    else:
        raw_location_value = raw_data.get('locations')

    final_data['locations'] = standardize_locations(raw_location_value)

    final_data.pop('city', None)
    final_data.pop('state', None)
    final_data.pop('country', None)

    return final_data


def check_schema(job_data: Dict[str, Any], target_schema: Dict) -> Tuple[bool, List[str]]:
    """
    Validates a transformed job data dictionary against a provided TARGET_SCHEMA.
    """
    errors = []

    for field, rules in target_schema.items():
        if rules.get("required") and field not in job_data:
            errors.append(f"Missing required field: '{field}'")

    if errors:
        return False, errors

    for field, value in job_data.items():
        if field in target_schema:
            rules = target_schema[field]
            expected_type_str = rules["type"]
            expected_type = TYPE_MAP.get(expected_type_str, str)

            if value is None:
                if not rules.get("nullable"):
                    errors.append(f"Field '{field}' cannot be null.")
                continue

            if expected_type_str == "datetime":
                if not validate_datetime_string(value):
                    errors.append(f"Field '{field}' is not a valid ISO datetime string. Got: {value}")
                continue

            if not isinstance(value, expected_type):
                errors.append(f"Field '{field}' has incorrect type. Expected {expected_type}, got {type(value)}.")

            if "allowed_values" in rules and value not in rules["allowed_values"]:
                errors.append(f"Field '{field}' has value '{value}', but only {rules['allowed_values']} are allowed.")

    return not errors, errors