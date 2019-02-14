""" Hello World.
Prints "Hello, world."
"""

from __future__ import print_function

from pyspark import SparkContext

import argparse
import datetime
import random

def main():

    parser = argparse.ArgumentParser()
    parser.add_argument('--use_mlops', type=int, dest='use_mlops', default=0)
    parser.add_argument("--echo_arg", dest='echo_arg', help="Program will print this argument", default="Hello, world")
    args = parser.parse_args()

    sc = SparkContext(appName="hello-world")
    if args.use_mlops > 0:
        ## MLOps start
        from parallelm.mlops import mlops
        from parallelm.mlops.stats_category import StatCategory

        # Initialize the mlops library
        mlops.init(sc)

        #
        mlops.set_stat("Testing set_stat", random.randint(1, 10))

        # Release mlops resources
        mlops.done()
        ## MLOps end

    sc.stop()
    print("{}: {}".format(args.echo_arg, datetime.datetime.now()))

if __name__ == "__main__":
    main()
