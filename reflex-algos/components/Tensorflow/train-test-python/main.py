from __future__ import print_function


import argparse
import logging
import sys

mlops_loaded = False
try:
    from parallelm.mlops import mlops
    from parallelm.mlops.e2e_tests import train_node
    mlops_loaded = True
except ImportError:
    pass


def parse_args():
    parser = argparse.ArgumentParser()

    parser.add_argument("--data-file", help="Data file to use as input")
    parser.add_argument("--output-model", help="Path of output model to create")
    parser.add_argument("--num-trees", type=int, default=3, help="Number of trees")
    parser.add_argument("--num-classes", type=int, default=2, help="Number of classes")
    parser.add_argument("--max-depth", type=int, default=4, help="Max depth to calculate")
    options = parser.parse_args()

    return options


def main():
    if not mlops_loaded:
        return

    root = logging.getLogger()
    root.setLevel(logging.INFO)

    ch = logging.StreamHandler(sys.stdout)
    ch.setLevel(logging.DEBUG)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    ch.setFormatter(formatter)
    root.addHandler(ch)

    options = parse_args()

    if options.output_model is None:
        raise Exception("Model output arg is None")

    mlops.init()
    train_node(options)
    mlops.done()


if __name__ == "__main__":
    main()
