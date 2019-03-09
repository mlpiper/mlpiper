:orphan:

.. _examples:

Examples
----------


Running spark mllib random forest and reporting statistics
=====================================================================

The following example shows how to run a pyspark program instrumented with mlops.
This type of program can be incorporated into MLOps as an Uploaded Components. When uploaded, the MLOps calls to
report statistics (set_stat) are ingested into the MLOps database and displayed on the UI. When not uploaded into
MLOps, the statistics are returned to stdout.

.. code-block:: python

    ...
    from parallelm.mlops import mlops as pm
    from parallelm.mlops import StatCategory as st
    ...
    sc = SparkContext(appName="PythonRandomForestClassificationExample")

    pm.init(sc)

    data = MLUtils.loadLibSVMFile(sc, options.data_file)
    (trainingData, testData) = data.randomSplit([0.7, 0.3])

    model = RandomForest.trainClassifier(trainingData,
                                         numClasses=options.num_classes,
                                         categoricalFeaturesInfo={},
                                         numTrees=options.num_trees,
                                         featureSubsetStrategy="auto",
                                         impurity='gini',
                                         maxDepth=options.max_depth,
                                         maxBins=32)

    # Evaluate model on test instances and compute test error
    predictions = model.predict(testData.map(lambda x: x.features))
    labelsAndPredictions = testData.map(lambda lp: lp.label).zip(predictions)
    testErr = labelsAndPredictions.filter(
        lambda lp: lp[0] != lp[1]).count() / float(testData.count())

    pm.set_stat("numTrees", options.num_trees, st.TIME_SERIES)
    pm.set_stat("numClasses", options.num_classes, st.TIME_SERIES)
    pm.set_stat("maxDepth", options.max_depth, st.TIME_SERIES)
    pm.set_stat("testError", testErr, st.TIME_SERIES)

    # Save and load model
    model.save(sc, options.output_model)
    print("Done saving model to {}".format(options.output_model))

    sc.stop()
    pm.done()



Here is another example, showing how to instrument a TensorFlow program to display cost and accuracy periodically as it
trains. When not uploaded into MLOps, the statistics are returned to stdout.

.. code-block::python

    ....
    from parallelm.mlops import mlops
    ....

    # Call initialize once.
    mlops.init()
    ....

    for i in range(training_steps):
        # Training code goes here.
        ...

        if (i % FLAGS.eval_step_interval) == 0
            # Calculate accuracy and cost here.
            ...

            mlops.set_stat("accuracy", accuracy * 100)
            mlops.set_stat("cost", cost)

    # Call done once.
    mlops.done()
