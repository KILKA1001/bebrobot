"""Utility functions for points formatting."""

def format_points(points: float) -> str:
    """Return a human-friendly representation of points.

    The number is rounded to two decimals and trailing zeros are removed.
    """
    formatted = f"{points:.2f}"
    formatted = formatted.rstrip("0").rstrip(".")
    return formatted
