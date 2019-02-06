#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

"""
Random Forest Classification Example.
"""
from __future__ import print_function

from pyspark import SparkContext
# $example on$
from pyspark.mllib.tree import RandomForest, RandomForestModel
from pyspark.mllib.util import MLUtils
from pyspark.mllib.linalg import Vectors

import argparse
# $example off$

from parallelm.mlops import mlops as pm
from parallelm.mlops import StatCategory as st


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

    sc = SparkContext(appName="PythonRandomForestClassificationExample")

    pm.init(sc)

    # $example on$
    # Load and parse the data file into an RDD of LabeledPoint.
    data = MLUtils.loadLibSVMFile(sc, options.data_file)
    # Split the data into training and test sets (30% held out for testing)
    (trainingData, testData) = data.randomSplit([0.7, 0.3])

    # Train a RandomForest model.
    #  Empty categoricalFeaturesInfo indicates all features are continuous.
    #  Note: Use larger numTrees in practice.
    #  Setting featureSubsetStrategy="auto" lets the algorithm choose.
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
    print('Test Error = ' + str(testErr))
    print('Learned classification forest model:')
    print(model.toDebugString())

    print("Using mlops to report statistics")

    # Adding multiple points (to see a graph in the ui)
    pm.set_stat("numTrees", options.num_trees, st.TIME_SERIES)
    pm.set_stat("numClasses", options.num_classes, st.TIME_SERIES)
    pm.set_stat("maxDepth", options.max_depth, st.TIME_SERIES)
    pm.set_stat("testError", testErr, st.TIME_SERIES)

    # TODO: this should be removed once we have better tests for mlops
    pm.set_stat("stat1", 1.0, st.TIME_SERIES)
    pm.set_stat("stat1", 2.0, st.TIME_SERIES)
    pm.set_stat("stat1", 3.0, st.TIME_SERIES)
    pm.set_stat("stat1", 4.0, st.TIME_SERIES)
    pm.set_stat("stat1", 5.0, st.TIME_SERIES)
    pm.set_stat("stat1", 6.0, st.TIME_SERIES)
    pm.set_stat("stat1", 7.0, st.TIME_SERIES)
    pm.set_stat("stat1", 8.0, st.TIME_SERIES)

    # String
    pm.set_stat("stat2", "str-value", st.TIME_SERIES)

    # Vec
    pm.set_stat("statvec", [4.5, 5.5, 6.6], st.TIME_SERIES)

    list_of_strings = []
    for x in range(1, 10000):
        list_of_strings.append("{},{},{}".format(x, x + 1, x + 2))

    rdd_of_str = sc.parallelize(list_of_strings)
    rdd = rdd_of_str.map(lambda line: Vectors.dense(line.split(",")))

    # Histograms and any input stats
    pm.set_stat("input", rdd, st.INPUT)

    print("Done reporting statistics")
    # Save and load model
    model.save(sc, options.output_model)
    print("Done saving model to {}".format(options.output_model))
    sameModel = RandomForestModel.load(sc, options.output_model)
    # $example off$

    sc.stop()
    pm.done()


if __name__ == "__main__":
    main()
