"""Constant values"""


class Modes:
    """Actions."""

    INSERT = "insert"
    CONVERT = "convert"
    QUERY = "query"


class FileTypes:
    """Output file types."""

    JSON = "json"
    CSV = "csv"


class OutputMode:
    """Output modes."""

    STDOUT = "stdout"
    FILE = "file"
    FILE_CHUNKS = "file-chunks"
