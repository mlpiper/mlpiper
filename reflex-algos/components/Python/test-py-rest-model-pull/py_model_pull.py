import argparse
import time

from parallelm.mlops import mlops as mlops

def parse_args():
    """
    Parse Arguments from component
    """
    parser = argparse.ArgumentParser()
    parser.add_argument("--input-model", help="Path to read input model from")
    options = parser.parse_args()
    return options


def main():
    mlops.init()

    options = parse_args()

    for i in range(10):
        model = mlops.get_last_approved_model()
        if model:
            mlops.set_stat("model fetched", 10)
            mlops.set_stat("fetched model size", model.metadata.size)
            print("FETCHED MODEL: {}".format(model.metadata))
            break
        else:
            mlops.set_stat("model fetched", 0)
            time.sleep(1)

    mlops.done()


if __name__ == "__main__":
    main()
