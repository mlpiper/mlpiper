import argparse
import tempfile
import pickle

from parallelm.mlops import mlops as mlops
from parallelm.mlops.models.model import ModelFormat

from datetime import datetime as dt

def parse_args():
    """
    Parse Arguments from component
    """
    parser = argparse.ArgumentParser()
    parser.add_argument("--output-model", help="Path of output model to create")
    parser.add_argument("--model-name", help="model name")
    parser.add_argument("--model-description", help="model desc")
    options = parser.parse_args()
    return options


def main():
    mlops.init()

    options = parse_args()

    # Save the model
    s = "Hello World"
    f = tempfile.NamedTemporaryFile()
    pickle.dump(s, f)
    f.flush()

    m = mlops.Model(name=options.model_name, model_format=ModelFormat.BINARY, description=options.model_description)
    m.set_annotations({"aaa":"my annotations"})
    m.set_model_path(f.name)
    mlops.publish_model(m)
    mlops.set_stat("model post time, minute", dt.now().minute)
    mlops.set_stat("posted model size", m.metadata.size)
    mlops.done()


if __name__ == "__main__":
    main()
