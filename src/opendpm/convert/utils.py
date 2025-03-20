"""Time formatting utilities."""

# Constants for time conversion
MINUTE = 60
HOUR = 60 * MINUTE
DAY = 24 * HOUR


def format_time(seconds: float) -> str:
    """Format time in seconds to a human-readable string."""
    if seconds < MINUTE:
        return f"{seconds:.2f} seconds"
    if seconds < HOUR:
        return f"{seconds / MINUTE:.2f} minutes"
    if seconds < DAY:
        return f"{seconds / HOUR:.2f} hours"
    return f"{seconds / DAY:.2f} days"
