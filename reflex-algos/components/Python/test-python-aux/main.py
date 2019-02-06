from __future__ import print_function

import argparse
import os
import sys
import time


from parallelm.mlops import mlops


def parse_args():

    parser = argparse.ArgumentParser()

    parser.add_argument("--arg1", help="Test argument 1")
    parser.add_argument("--input-model", help="Path to read input model from")
    parser.add_argument("--exit-value", type=int, default=0, help="Exit value")
    parser.add_argument("--iter", type=int, default=20, help="How many 1sec iterations to perform")
    parser.add_argument("--use-mlops", type=int, default=1, help="Use mlops while running")
    options = parser.parse_args()
    return options


def main():

    print("args: {}".format(sys.argv))
    options = parse_args()
    print("- inside test-python-aux Running main.py")
    print("arg1:         {}".format(options.arg1))
    print("input_model:  {}".format(options.input_model))
    print("use-mlops:    {}".format(options.use_mlops))
    print("iter:         {}".format(options.iter))
    print("exit_value:   {}".format(options.exit_value))

    print("Calling mlops.init()")
    if options.use_mlops:
        mlops.init()

    # Some output - to test logs
    for idx in range(options.iter):
        print("stdout - Idx {}".format(idx))
        print("stderr - Idx {}".format(idx), file=sys.stderr)
        if options.use_mlops:
            mlops.set_stat("aux_stat", 1)
        time.sleep(1)

    if options.use_mlops:
        mlops.done()

    # Exit status
    if options.exit_value >= 0:
        print("About to exit with value: {}".format(options.exit_value))
        sys.exit(options.exit_value)
    else:
        print("About to raise exception: {}".format(options.exit_value))
        raise Exception("Exiting main using exception")


if __name__ == "__main__":
    main()
