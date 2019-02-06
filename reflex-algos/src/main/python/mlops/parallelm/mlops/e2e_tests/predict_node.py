from parallelm.mlops import mlops as pm


def predict_node(options):
    print("Predict test")
    if options.input_model is not None:
        try:
            f = open(options.input_model, "r")
            model = f.read()

            print("model content: [{}]".format(model))
            pm.set_stat("model_file", 1)

            model = pm.current_model()
            assert model is not None
            print("Model object: [{}]".format(model))

        except Exception as e:
            print("Model not found")
            print("Got exception: " + str(e))
            pm.set_stat("model_file", 0)

    # Adding multiple points (to see a graph in the ui)
    pm.set_stat("numTrees", options.num_trees)
    pm.set_stat("numClasses", options.num_classes)
    pm.set_stat("maxDepth", options.max_depth)
    pm.set_stat("testError", 0.75)
    # TODO: this should be removed once we have better tests for mlops
    pm.set_stat("stat1", 1.0)
    # String

    print("Done reporting statistics")
    # Save and load model

