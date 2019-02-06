
import pytest

from datetime import datetime, timedelta
from parallelm.mlops.ion.ion_builder import  IONBuilder
from parallelm.mlops.utils import time_to_str_timestamp_milli


def test_datetime_timestamps():

    ts = time_to_str_timestamp_milli(datetime.now())

    assert ts is not None
