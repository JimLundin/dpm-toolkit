"""Time formatting utilities."""

# Constants for time conversion
SECONDS_PER_MINUTE = 60
SECONDS_PER_HOUR = 60 * SECONDS_PER_MINUTE


def format_duration(seconds: float) -> str:
    """Format time in seconds to a human-readable string."""
    if seconds < SECONDS_PER_MINUTE:
        return f"{seconds:.0f} seconds"

    minutes, remaining_seconds = divmod(seconds, SECONDS_PER_MINUTE)
    return f"{minutes:.0f} minutes and {remaining_seconds:.0f} seconds"
