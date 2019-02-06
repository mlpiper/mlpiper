from __future__ import print_function

from pyspark import SparkContext

import argparse

from parallelm.mlops import mlops as pm
from parallelm.mlops.e2e_tests import train_node

from gen_data_dist import gen_data_dist_stats


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
    options = parse_args()

    if options.output_model is None:
        raise Exception("Model output arg is None")

    sc = SparkContext(appName="train-test")

    pm.init(sc)

    train_node(options)

    # In addition we run kmeans and generate data distribution histograms
    # This is spark specific code so it is called here and not in the train_node code.
    # We will not save the model but just the stats

    gen_data_dist_stats(sc)

    sc.stop()
    pm.done()


if __name__ == "__main__":
    main()
