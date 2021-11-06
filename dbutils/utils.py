"""Util methods."""
import argparse
from typing import List


def str2bool(value: str) -> bool:
    """str2bool

    Converts string to boolean for argparse
    https://stackoverflow.com/a/43357954
    """
    if isinstance(value, bool):
        return value
    if value.lower() in ("yes", "true", "t", "y", "1"):
        return True
    elif value.lower() in ("no", "false", "f", "n", "0"):
        return False
    else:
        raise argparse.ArgumentTypeError(f"Boolean value expected, given {value}")


def get_comma_separated_fields(s: str) -> List[str]:
    """get_comma_separated_fields

    For given comma-separated values, returns list of values.
    """
    return [val.strip() for val in s.split(",") if val.strip() != ""]
