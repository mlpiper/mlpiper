from __future__ import print_function

from pyspark import SparkContext
from datetime import datetime, timedelta
from parallelm.mlops import mlops as pm
from parallelm.mlops import StatCategory as st
import numpy as np
import math
import pandas as pd
import argparse
from random import *

def main():
    sc = SparkContext(appName="fake ab")

    pm.init(sc)
    print("Test")

    try:
        pm.set_stat("samples", 10000, st.TIME_SERIES)
        pm.set_stat("conversions", randint(1, 10000), st.TIME_SERIES)

    except Exception as e:
        print("Got exception while getting stats: {}".format(e))
        pm.set_stat("error", 1, st.TIME_SERIES)

    sc.stop()
    pm.done()


if __name__ == "__main__":
    main()
