"""
Validation utilities for PUVI Oil Manufacturing System
Contains functions for safe data type conversions and validation
"""

from decimal import Decimal, InvalidOperation

def safe_decimal(value, default=0):
    """
    Safely convert a value to Decimal, handling various edge cases
    
    Args:
        value: The value to convert (can be string, number, None, etc.)
        default: Default value if conversion fails (default: 0)
    
    Returns:
        Decimal: The converted value or default
    
    Examples:
        safe_decimal("123.45") -> Decimal("123.45")
        safe_decimal("") -> Decimal("0")
        safe_decimal(None) -> Decimal("0")
        safe_decimal("abc") -> Decimal("0")
    """
    try:
        # Handle None, empty string, or string with only whitespace
        if value is None or value == '' or (isinstance(value, str) and value.strip() == ''):
            return Decimal(str(default))
        
        # Handle the string "null" (from JavaScript)
        if isinstance(value, str) and value.lower() == 'null':
            return Decimal(str(default))
        
        # Convert to string first to handle various numeric types
        return Decimal(str(value))
    except (ValueError, TypeError, InvalidOperation) as e:
        print(f"Warning: Could not convert '{value}' to Decimal. Using default: {default}. Error: {e}")
        return Decimal(str(default))


def safe_float(value, default=0):
    """
    Safely convert a value to float, handling various edge cases
    
    Args:
        value: The value to convert
        default: Default value if conversion fails
    
    Returns:
        float: The converted value or default
    
    Examples:
        safe_float("123.45") -> 123.45
        safe_float("") -> 0.0
        safe_float(None) -> 0.0
        safe_float("abc") -> 0.0
    """
    try:
        # Handle None, empty string, or string with only whitespace
        if value is None or value == '' or (isinstance(value, str) and value.strip() == ''):
            return float(default)
        
        # Handle the string "null" (from JavaScript)
        if isinstance(value, str) and value.lower() == 'null':
            return float(default)
            
        return float(value)
    except (ValueError, TypeError) as e:
        print(f"Warning: Could not convert '{value}' to float. Using default: {default}. Error: {e}")
        return float(default)


def safe_int(value, default=0):
    """
    Safely convert a value to integer, handling various edge cases
    
    Args:
        value: The value to convert
        default: Default value if conversion fails
    
    Returns:
        int: The converted value or default
    """
    try:
        if value is None or value == '' or (isinstance(value, str) and value.strip() == ''):
            return int(default)
        
        if isinstance(value, str) and value.lower() == 'null':
            return int(default)
            
        # Handle floats by converting to int
        return int(float(value))
    except (ValueError, TypeError) as e:
        print(f"Warning: Could not convert '{value}' to int. Using default: {default}. Error: {e}")
        return int(default)


def validate_positive_number(value, field_name):
    """
    Validate that a value is a positive number
    
    Args:
        value: The value to validate
        field_name: Name of the field (for error messages)
    
    Returns:
        tuple: (is_valid, error_message)
    """
    num_value = safe_decimal(value)
    if num_value <= 0:
        return False, f"{field_name} must be greater than 0"
    return True, None


def validate_percentage(value, field_name, max_percent=100):
    """
    Validate that a value is a valid percentage
    
    Args:
        value: The value to validate
        field_name: Name of the field (for error messages)
        max_percent: Maximum allowed percentage (default: 100)
    
    Returns:
        tuple: (is_valid, error_message)
    """
    num_value = safe_decimal(value)
    if num_value < 0:
        return False, f"{field_name} cannot be negative"
    if num_value > max_percent:
        return False, f"{field_name} cannot exceed {max_percent}%"
    return True, None


def validate_required_fields(data, required_fields):
    """
    Validate that all required fields are present and not empty
    
    Args:
        data: Dictionary containing the data
        required_fields: List of required field names
    
    Returns:
        tuple: (is_valid, missing_fields)
    """
    missing_fields = []
    
    for field in required_fields:
        if field not in data or data[field] is None or data[field] == '':
            missing_fields.append(field)
    
    return len(missing_fields) == 0, missing_fields
