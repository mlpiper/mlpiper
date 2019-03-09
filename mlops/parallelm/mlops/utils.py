from datetime import datetime

from parallelm.mlops.mlops_exception import MLOpsException


def datetime_to_timestamp_milli(datetime_object):
    """
    Return a number representing the milliseconds since the epoch.
    :param datetime_object:  datetime object
    :type datetime_object: datetime
    :return: integer representing the milliseconds since the epoch
    :raises MLOpsException for invalid arguments
    """
    if isinstance(datetime_object, datetime):

        timestamp = (datetime_object - datetime(1970, 1, 1)).total_seconds() * 1000
        return int(timestamp)

    # Note: other objects/str support can be added here.
    raise MLOpsException("Time object is not datetime.datetime object")


def time_to_str_timestamp_milli(time_object):
    """
    Convert a time representing object to a millisecond timestamp.
    :param time_object:
    :return: A string containing a long number representing a timestamp object in milliseconds.
    """
    return str(datetime_to_timestamp_milli(time_object))
