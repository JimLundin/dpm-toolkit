"""Time formatting utilities."""

# Constants for time conversion
MINUTE = 60
HOUR = 60 * MINUTE
DAY = 24 * HOUR


def format_time(seconds: float) -> str:
    """Format time in seconds to a human-readable string."""
    if seconds < MINUTE:
        return f"{seconds:.0f} seconds"
    if seconds < HOUR:
        minutes, seconds = divmod(seconds, MINUTE)
        return f"{minutes:.0f} minutes and {seconds:.0f} seconds"
    if seconds < DAY:
        hours, minutes = divmod(seconds, HOUR)
        return f"{hours:.0f} hours and {minutes:.0f} minutes"

    days, hours = divmod(seconds, DAY)
    return f"{days:.0f} days and {hours:.0f} hours"
