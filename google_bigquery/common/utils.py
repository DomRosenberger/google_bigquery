"""Utils functions."""
import datetime


def MillisecondsSinceEpoch(hours):
    """Returns time in milliseconds since epoch for given time in hours.

    Args:
        hours: Int, the hours of the future timestamp.
    Returns:
        Int, the future timestamp in milliseconds.

    """
    hours = datetime.datetime.now() + datetime.timedelta(hours=hours)
    epoch = datetime.datetime.utcfromtimestamp(0)
    delta = hours - epoch
    return int(delta.total_seconds() * 1000)
