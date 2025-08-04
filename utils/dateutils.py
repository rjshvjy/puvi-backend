"""
Date utilities for PUVI Oil Manufacturing System
Handles date conversions between different formats and database storage
"""

from datetime import datetime, date, timedelta

def date_to_day_number(date_string):
    """
    Convert date string to day number since epoch (1970-01-01)
    
    Args:
        date_string: Date in DD-MM-YYYY or YYYY-MM-DD format
    
    Returns:
        int: Number of days since epoch
    
    Examples:
        date_to_day_number("04-08-2025") -> 20304
        date_to_day_number("2025-08-04") -> 20304
    """
    # Handle both formats for compatibility
    if '-' in date_string and len(date_string.split('-')[0]) == 4:
        # YYYY-MM-DD format from HTML date input
        date_obj = datetime.strptime(date_string, '%Y-%m-%d')
    else:
        # DD-MM-YYYY format (Indian standard)
        date_obj = datetime.strptime(date_string, '%d-%m-%Y')
    
    epoch = datetime(1970, 1, 1)
    return (date_obj - epoch).days


def parse_date(date_string):
    """
    Parse date from various formats to integer (days since epoch)
    Accepts: YYYY-MM-DD, DD-MM-YYYY, DD/MM/YYYY, or already an integer
    
    Args:
        date_string: Date in various formats or None
    
    Returns:
        int: Days since epoch, or None if input is None/empty
    
    Examples:
        parse_date("2025-08-04") -> 20304
        parse_date("04-08-2025") -> 20304
        parse_date("04/08/2025") -> 20304
        parse_date(20304) -> 20304
        parse_date("") -> None
    """
    if not date_string:
        return None
    
    # If already an integer, return it
    try:
        return int(date_string)
    except (ValueError, TypeError):
        pass
    
    # Try different date formats
    formats = [
        '%Y-%m-%d',  # ISO format (from HTML date inputs)
        '%d-%m-%Y',  # Indian format with dash
        '%d/%m/%Y',  # Indian format with slash
    ]
    
    for fmt in formats:
        try:
            dt = datetime.strptime(str(date_string), fmt).date()
            # Convert to days since epoch (1970-01-01)
            epoch = date(1970, 1, 1)
            return (dt - epoch).days
        except ValueError:
            continue
    
    raise ValueError(f"Unable to parse date: {date_string}")


def integer_to_date(days_since_epoch, format='%d-%m-%Y'):
    """
    Convert integer (days since epoch) to formatted date string
    
    Args:
        days_since_epoch: Number of days since 1970-01-01
        format: Output format (default: DD-MM-YYYY)
    
    Returns:
        str: Formatted date string, or empty string if invalid
    
    Examples:
        integer_to_date(20304) -> "04-08-2025"
        integer_to_date(20304, '%Y-%m-%d') -> "2025-08-04"
        integer_to_date(None) -> ""
    """
    if days_since_epoch is None:
        return ''
    
    try:
        # Convert integer to date
        epoch = date(1970, 1, 1)
        dt = epoch + timedelta(days=int(days_since_epoch))
        # Format as requested (default: DD-MM-YYYY)
        return dt.strftime(format)
    except:
        return ''


def get_current_day_number():
    """
    Get current date as day number since epoch
    
    Returns:
        int: Current day number
    """
    return (datetime.now().date() - date(1970, 1, 1)).days


def format_date_for_display(date_value):
    """
    Format any date value for display in Indian format (DD-MM-YYYY)
    
    Args:
        date_value: Can be string, datetime, date, or integer (days since epoch)
    
    Returns:
        str: Date in DD-MM-YYYY format
    """
    if isinstance(date_value, int):
        return integer_to_date(date_value)
    elif isinstance(date_value, (datetime, date)):
        return date_value.strftime('%d-%m-%Y')
    elif isinstance(date_value, str):
        # Parse and reformat
        try:
            days = parse_date(date_value)
            return integer_to_date(days)
        except:
            return date_value
    return ''


def validate_date_range(start_date, end_date):
    """
    Validate that start_date is before or equal to end_date
    
    Args:
        start_date: Start date (string or int)
        end_date: End date (string or int)
    
    Returns:
        tuple: (is_valid, error_message)
    """
    try:
        start_days = parse_date(start_date) if not isinstance(start_date, int) else start_date
        end_days = parse_date(end_date) if not isinstance(end_date, int) else end_date
        
        if start_days > end_days:
            return False, "Start date must be before or equal to end date"
        return True, None
    except Exception as e:
        return False, f"Invalid date format: {str(e)}"


def get_financial_year(date_value=None):
    """
    Get financial year for a given date (April to March)
    
    Args:
        date_value: Date to check (default: current date)
    
    Returns:
        str: Financial year in format "YYYY-YY" (e.g., "2025-26")
    """
    if date_value is None:
        dt = datetime.now().date()
    else:
        days = parse_date(date_value) if not isinstance(date_value, int) else date_value
        dt = date(1970, 1, 1) + timedelta(days=days)
    
    if dt.month >= 4:
        return f"{dt.year}-{str(dt.year + 1)[2:]}"
    else:
        return f"{dt.year - 1}-{str(dt.year)[2:]}"


def get_month_year(date_value=None):
    """
    Get month and year from a date value
    
    Args:
        date_value: Date to check (default: current date)
    
    Returns:
        tuple: (month_name, year) e.g., ("August", 2025)
    """
    if date_value is None:
        dt = datetime.now().date()
    else:
        days = parse_date(date_value) if not isinstance(date_value, int) else date_value
        dt = date(1970, 1, 1) + timedelta(days=days)
    
    return dt.strftime('%B'), dt.year
