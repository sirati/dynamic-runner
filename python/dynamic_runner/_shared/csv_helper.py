import csv
import sys


def increase_csv_field_size_limit() -> None:
    """Increase CSV field size limit to maximum possible value.

    This prevents _csv.Error when reading CSV files with large fields.
    Tries to set the limit to sys.maxsize, reducing by factor of 10 if OverflowError occurs.
    """
    maxInt = sys.maxsize
    while True:
        try:
            csv.field_size_limit(maxInt)
            break
        except OverflowError:
            maxInt = int(maxInt / 10)
