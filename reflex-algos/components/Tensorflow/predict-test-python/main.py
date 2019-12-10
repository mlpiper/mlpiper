from __future__ import print_function


import argparse

mlops_loaded = False
try:
    from parallelm.mlops import mlops as pm
    from parallelm.mlops.e2e_tests import predict_node
    mlops_loaded = True
except ImportError:
    pass

def parse_args():

    parser = argparse.ArgumentParser()

    parser.add_argument("--data-file", help="Data file to use as input")
    parser.add_argument("--input-model", help="Path of input model to use")
    parser.add_argument("--num-trees", type=int, default=3, help="Number of trees")
    parser.add_argument("--num-classes", type=int, default=2, help="Number of classes")
    parser.add_argument("--max-depth", type=int, default=4, help="Max depth to calculate")
    options = parser.parse_args()

    return options


def main():
    if not mlops_loaded:
        return

    options = parse_args()

    pm.init()
    predict_node(options)
    pm.done()


if __name__ == "__main__":
    main()
