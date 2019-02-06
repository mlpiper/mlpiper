from datetime import datetime


class TimeFunctions:
    """This class provides multiple functions that are used for the MCenter time capture
    format converter

    """
    def __init__(self):
        """Initialize the parameters of the time Filters."""

    def time_to_str_timestamp_milli(self, time_object):
        """
        Convert a time representing object to a millisecond timestamp.
        :param time_object:
        :return: A string containing a long number representing a timestamp object in milliseconds.
        """
        return str(self._datetime_to_timestamp_milli(time_object))

    def _datetime_to_timestamp_milli(self, datetime_object):
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

    def time_filter(self, df, min_time_window, max_time_window):
        """
        The function filters a dataframe between min and max time

        :param self:
        :param df: Dataframe to Filter
        :param min_time_window: Min time
        :param max_time_window: Max time
        :return:
        """

        df['timeNum'] = df['time'].astype(float)
        out_df = df[(df['timeNum'] <= int(max_time_window)) & (df['timeNum'] >= int(min_time_window))]
        if out_df.shape[0] > 0:
            out_df = out_df.reset_index()
        return out_df.drop('timeNum', axis=1)
