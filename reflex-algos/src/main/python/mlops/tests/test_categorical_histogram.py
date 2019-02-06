import numpy as np
import pandas as pd

from parallelm.mlops.stats.health.categorical_hist_stat import CategoricalHistogram
from parallelm.mlops.stats.health.general_hist_stat import GeneralHistogramStat
from parallelm.mlops.stats.health.histogram_data_objects import CategoricalHistogramDataObject


def test_generation_of_categorical_hist():
    categorical_features_values = np.array([[1, 2], [3, 4], [5, 6], [7, 8]])
    categorical_features_names = ["c0", "c1"]

    categorical_histogram = CategoricalHistogram()

    categorical_histogram.fit(training_feature_names=categorical_features_names,
                              training_feature_values=categorical_features_values,
                              num_bins=13,
                              pred_bins=None)

    hist_rep = categorical_histogram.get_feature_histogram_rep()

    assert len(hist_rep) == 2

    c0_stat = hist_rep[0]
    assert isinstance(c0_stat, CategoricalHistogramDataObject)
    assert c0_stat.get_feature_name() == "c0"
    assert False not in (c0_stat.get_bins() == [0.25, 0.25, 0.25, 0.25])

    assert c0_stat.get_edges() == ['1', '3', '5', '7']
    assert c0_stat.get_edge_list() == ['1', '3', '5', '7']

    c1_stat = hist_rep[1]
    assert c1_stat.get_feature_name() == "c1"
    assert False not in (c1_stat.get_bins() == [0.25, 0.25, 0.25, 0.25])
    assert c1_stat.get_edges() == ['2', '4', '6', '8']
    assert c1_stat.get_edge_list() == ['2', '4', '6', '8']
    assert isinstance(c1_stat, CategoricalHistogramDataObject)


def test_generation_of_continuous_hist_for_pandas():
    array_of_data = np.array(
        [[1, 2, 3, "A"], [2, 3, 4, "B"], [2, 3, 4, "A"], [3, 4, 4, "B"], [2, 3, 4, "B"], [1, 24, 34, "B"],
         [1, 24, 34, "B"], [1, 24, 34, "B"]])
    feature_length = len(array_of_data[0])

    array_of_features = []
    for i in range(feature_length):
        array_of_features.append("c" + str(i))

    pd_data = pd.DataFrame(data=array_of_data, columns=array_of_features)

    features_values = np.array(pd_data.values)
    features_names = list(pd_data.columns)

    # generating general stats like categorical/continuous features and contender histograms.
    general_hist_stat = GeneralHistogramStat()
    general_hist_stat.max_cat_unique_values = 3
    general_hist_stat \
        .create_and_set_general_stat(set_of_features_values=features_values,
                                     set_of_features_names=features_names,
                                     model_stat=None)

    # For Categorical Values
    # categorical feature names
    categorical_features_names = general_hist_stat.set_of_categorical_features

    # categorical feature values in order of names
    categorical_feature_values_array = []
    for each_categorical_f_names in categorical_features_names:
        categorical_feature_values_array.append(
            features_values[:, features_names.index(each_categorical_f_names)])
    categorical_features_values = np.array(categorical_feature_values_array).T

    # predefined bins of contender categorical hist
    pred_bins_categorical_hist = general_hist_stat.contender_categorical_hist_bins
    contender_categorical_histogram_representation = general_hist_stat.contender_categorical_histogram

    # generating categorical histogram if categorical_features_values exists.
    current_categorical_histogram_representation = None

    if len(categorical_features_values) > 0:
        current_categorical_histogram = CategoricalHistogram() \
            .fit(categorical_features_values,
                 categorical_features_names,
                 num_bins=13,
                 pred_bins=pred_bins_categorical_hist)

        current_categorical_histogram_representation = \
            current_categorical_histogram.get_feature_histogram_rep()

        c3_stat = current_categorical_histogram_representation[0]
        assert isinstance(c3_stat, CategoricalHistogramDataObject)
        assert c3_stat.get_feature_name() == "c3"
        assert False not in (c3_stat.get_bins() == [0.25, 0.75])
        #
        assert c3_stat.get_edges() == ['A', 'B']
        assert c3_stat.get_edge_list() == ['A', 'B']
