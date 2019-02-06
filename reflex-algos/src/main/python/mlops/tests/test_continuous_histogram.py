import numpy as np
import pandas as pd

from parallelm.mlops.stats.health.continuous_hist_stat import ContinuousHistogram, ContinuousHistogramDataObject
from parallelm.mlops.stats.health.general_hist_stat import GeneralHistogramStat


def test_generation_of_continuous_hist():
    continuous_features_values = np.array([[2, 2], [0, 6]])
    continuous_features_names = ["c0", "c1"]

    continuous_histogram = ContinuousHistogram()

    continuous_histogram.fit(training_feature_names=continuous_features_names,
                             training_feature_values=continuous_features_values,
                             num_bins=13,
                             pred_bins=None)

    hist_rep = continuous_histogram.get_feature_histogram_rep()

    assert len(hist_rep) == 2

    c0_stat = hist_rep[0]
    assert isinstance(c0_stat, ContinuousHistogramDataObject)
    assert c0_stat.get_feature_name() == "c0"
    assert False not in (c0_stat.get_bins() == [0, 0, 0, 0.5, 0, 0, 0, 0, 0, 0.5, 0, 0, 0])
    assert False not in (np.ceil(c0_stat.get_edge_list() * 100) == np.ceil(
        [-float("inf"), -1.0, -0.6363636363636364, -0.2727272727272727,
         0.09090909090909083,
         0.4545454545454546, 0.8181818181818183, 1.1818181818181817, 1.5454545454545454,
         1.9090909090909092, 2.272727272727273, 2.6363636363636367, 3.0, float("inf")] * 100))

    c1_stat = hist_rep[1]
    assert c1_stat.get_feature_name() == "c1"
    assert False not in (c1_stat.get_bins() == [0, 0, 0, 0.5, 0, 0, 0, 0, 0, 0.5, 0, 0, 0])
    assert False not in (np.ceil(c1_stat.get_edge_list() * 100) == np.ceil(
        [-float("inf"), 0.0, 0.7272727272727273, 1.4545454545454546,
         2.1818181818181817,
         2.909090909090909, 3.6363636363636367, 4.363636363636363,
         5.090909090909091,
         5.818181818181818, 6.545454545454546, 7.272727272727273, 8.0,
         float("inf")] * 100))

    assert isinstance(c1_stat, ContinuousHistogramDataObject)


def test_generation_of_continuous_hist_having_special_chars():
    continuous_features_values = np.array(
        [["2A", 2, "3", 4, "A"],
         ["0C", 6, "9", 8, "B"],
         ["2", 2, "3", 4, "C"],
         ["A0", 6, "9", 8, "D"],
         ["2B", 2, "3", 4, "E"],
         ["0W", 6, "9", 8, "F"]])

    continuous_features_names = ["c0", "c1", "c2", "c3", "c4"]

    continuous_histogram = ContinuousHistogram()

    continuous_histogram.fit(training_feature_names=continuous_features_names,
                             training_feature_values=continuous_features_values,
                             num_bins=13,
                             pred_bins=None)

    hist_rep = continuous_histogram.get_feature_histogram_rep()

    assert len(hist_rep) == 3

    # creation of c1 is expected to be like that
    c1_stat = hist_rep[0]
    assert c1_stat.get_feature_name() == "c1"
    assert isinstance(c1_stat, ContinuousHistogramDataObject)

    assert False not in (c1_stat.get_bins() == [0, 0, 0, 0.5, 0, 0, 0, 0, 0, 0.5, 0, 0, 0])
    assert False not in (np.ceil(c1_stat.get_edge_list() * 100) == np.ceil(
        [-float("inf"), 0.0, 0.7272727272727273, 1.4545454545454546,
         2.1818181818181817,
         2.909090909090909, 3.6363636363636367, 4.363636363636363,
         5.090909090909091,
         5.818181818181818, 6.545454545454546, 7.272727272727273, 8.0,
         float("inf")] * 100))

    # c2 is string of numbers. Since it is classified as continuous, we should at least give it a go.
    c2_stat = hist_rep[1]
    assert c2_stat.get_feature_name() == "c2"
    assert isinstance(c2_stat, ContinuousHistogramDataObject)

    assert False not in (c2_stat.get_bins() == [0, 0, 0, 0.5, 0, 0, 0, 0, 0, 0.5, 0, 0, 0])
    assert False not in (np.ceil(c2_stat.get_edge_list() * 100) == np.ceil(
        [-float("inf"), 0.0, 1.0909090909090908, 2.1818181818181817, 3.2727272727272725, 4.363636363636363,
         5.454545454545454, 6.545454545454545, 7.636363636363636, 8.727272727272727, 9.818181818181817,
         10.909090909090908, 12.0,
         float("inf")] * 100))

    # creation of c3 is expected to be like that
    c3_stat = hist_rep[2]
    assert c3_stat.get_feature_name() == "c3"
    assert isinstance(c3_stat, ContinuousHistogramDataObject)

    assert False not in (c3_stat.get_bins() == [0, 0, 0, 0.5, 0, 0, 0, 0, 0, 0.5, 0, 0, 0])

    assert False not in (np.ceil(c3_stat.get_edge_list() * 100) == np.ceil(
        [-float("inf"), 2.0, 2.7272727272727275, 3.454545454545455, 4.1818181818181825, 4.90909090909091,
         5.636363636363638, 6.363636363636365, 7.090909090909093, 7.81818181818182, 8.545454545454547,
         9.272727272727275, 10.0,
         float("inf")] * 100))


def test_generation_of_continuous_hist_for_constant_vals():
    continuous_features_values = np.array([[2, 2], [2, 6], [2, 10]])
    continuous_features_names = ["c0", "c1"]

    continuous_histogram = ContinuousHistogram()

    continuous_histogram.fit(training_feature_names=continuous_features_names,
                             training_feature_values=continuous_features_values,
                             num_bins=13,
                             pred_bins=None)

    hist_rep = continuous_histogram.get_feature_histogram_rep()
    c0_stat = hist_rep[0]
    assert isinstance(c0_stat, ContinuousHistogramDataObject)
    assert c0_stat.get_feature_name() == "c0"

    assert False not in (c0_stat.get_bins() == [0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, ])
    assert c0_stat.get_edge_list()[1] == 1.9
    assert c0_stat.get_edge_list()[12] == 2.1


def test_generation_of_continuous_hist_for_pandas():
    array_of_data = np.array(
        [[1, 2, 3, "A"], [2, 3, 4, "B"], [2, 3, 4, "A"], [3, 4, 4, "B"], [2, 3, 4, "B"], [1, 24, 34, "B"]])
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

    # For Continuous Values
    # continuous feature names
    continuous_features_names = general_hist_stat.set_of_continuous_features

    # continuous feature values in order of names
    continuous_feature_values_array = []
    for each_continuous_f_names in continuous_features_names:
        continuous_feature_values_array.append(
            features_values[:, features_names.index(each_continuous_f_names)])
    continuous_features_values = np.array(continuous_feature_values_array).T

    # predefined bins of contender continuous hist
    pred_bins_continuous_hist = general_hist_stat.contender_continuous_hist_bins
    contender_continuous_histogram_representation = general_hist_stat.contender_continuous_histogram

    # generating continuous histogram if continuous_features_values exists.
    current_continuous_histogram_representation = None

    if len(continuous_features_values) > 0:
        current_continuous_histogram = ContinuousHistogram() \
            .fit(continuous_features_values,
                 continuous_features_names,
                 num_bins=13,
                 pred_bins=pred_bins_continuous_hist)

        current_continuous_histogram_representation = \
            current_continuous_histogram.get_feature_histogram_rep()

        c0_stat = current_continuous_histogram_representation[0]
        assert isinstance(c0_stat, ContinuousHistogramDataObject)
        assert False not in (
                np.ceil(c0_stat.get_bins() * 100) == np.ceil(
            np.array([0.0, 0.0, 0.0, 0.33333333, 0.0, 0.0, 0.0, 0.5, 0.0, 0.0, 0.0, 0.16666667, 0]) * 100))

        assert False not in (np.ceil(c0_stat.get_edge_list() * 100) == np.ceil(
            [-float("inf"), 1.0, 1.0, 1.0, 2.0, 2.0, 2.0, 2.0, 3.0, 3.0, 3.0, 3.0, 4.0, float("inf")] * 100))


def test_generation_of_continuous_hist_for_pandas_having_weird_vals():
    array_of_data = np.array(
        [["2A", 2, "3", 4, "A"],
         ["0C", 6, "4.5", 6, "B"],
         ["2", 2, "3", 4, "C"],
         ["A0", 6, "4.5", 6, "D"],
         ["2B", 2, "3", 4, "E"],
         ["0W", 6, "4.5", 6, "F"]])

    feature_length = len(array_of_data[0])

    array_of_features = []
    for i in range(feature_length):
        array_of_features.append("c" + str(i))

    pd_data = pd.DataFrame(data=array_of_data, columns=array_of_features)

    features_values = np.array(pd_data.values)
    features_names = list(pd_data.columns)

    model_stat_with_both_stat = [
        u'{"data":"{\\"c4\\": [{\\"A\\": 0.0}, {\\"B\\": 0.16666666666666666}, {\\"C\\": 0.0}]}","graphType":"BARGRAPH","timestamp":1536614947113726976,"mode":"INSTANT","name":"categoricalDataHistogram","type":"Health"}',
        u'{"data":"{\\"c1\\": [{\\"1 to 3\\": 0.0}, {\\"3 to 5\\": 0.16666666666666666}, {\\"5 to 7\\": 0.0}], \\"c2\\": [{\\"2 to 3.5\\": 0.0}, {\\"3.5 to 5\\": 0.16666666666666666}, {\\"5 to 6.5\\": 0.0}], \\"c3\\": [{\\"1 to 3\\": 0.0}, {\\"3 to 5\\": 0.16666666666666666}, {\\"5 to 7\\": 0.16666666666666666}], \\"c0\\": [{\\"1 to 3\\": 0.0}, {\\"3 to 5\\": 0.16666666666666666}, {\\"5 to 7\\": 0.0}]}","graphType":"BARGRAPH","timestamp":1536614947113726976,"mode":"INSTANT","name":"continuousDataHistogram","type":"Health"}'
    ]

    # generating general stats like categorical/continuous features and contender histograms.
    general_hist_stat = GeneralHistogramStat()
    general_hist_stat.max_cat_unique_values = 3
    general_hist_stat \
        .create_and_set_general_stat(set_of_features_values=features_values,
                                     set_of_features_names=features_names,
                                     model_stat=model_stat_with_both_stat)

    # For Continuous Values
    # continuous feature names
    continuous_features_names = general_hist_stat.set_of_continuous_features

    # continuous feature values in order of names
    continuous_feature_values_array = []
    for each_continuous_f_names in continuous_features_names:
        continuous_feature_values_array.append(
            features_values[:, features_names.index(each_continuous_f_names)])
    continuous_features_values = np.array(continuous_feature_values_array).T

    # predefined bins of contender continuous hist
    pred_bins_continuous_hist = general_hist_stat.contender_continuous_hist_bins
    contender_continuous_histogram_representation = general_hist_stat.contender_continuous_histogram

    # generating continuous histogram if continuous_features_values exists.
    current_continuous_histogram_representation = None

    if len(continuous_features_values) > 0:
        current_continuous_histogram = ContinuousHistogram() \
            .fit(continuous_features_values,
                 continuous_features_names,
                 num_bins=13,
                 pred_bins=pred_bins_continuous_hist)

        current_continuous_histogram_representation = \
            current_continuous_histogram.get_feature_histogram_rep()

        for each_stat in current_continuous_histogram_representation:
            if each_stat.get_feature_name == "c1":
                assert each_stat.get_feature_name() == "c1"

                assert isinstance(each_stat, ContinuousHistogramDataObject)
                assert False not in (
                        np.ceil(each_stat.get_bins() * 100) == np.ceil(
                    np.array([0.5, 0, 0.5]) * 100))

                assert False not in (np.ceil(each_stat.get_edge_list() * 100) == np.ceil(
                    [1.0, 3.0, 5.0, 7.0] * 100))

            elif each_stat.get_feature_name == "c2":
                assert each_stat.get_feature_name() == "c2"

                assert isinstance(each_stat, ContinuousHistogramDataObject)
                assert False not in (
                        np.ceil(each_stat.get_bins() * 100) == np.ceil(
                    np.array([0.5, 0.5, 0]) * 100))
                assert False not in (np.ceil(each_stat.get_edge_list() * 100) == np.ceil(
                    [2.0, 3.5, 5.0, 6.5] * 100))

            elif each_stat.get_feature_name == "c3":
                assert each_stat.get_feature_name() == "c3"

                assert isinstance(each_stat, ContinuousHistogramDataObject)
                assert False not in (
                        np.ceil(each_stat.get_bins() * 100) == np.ceil(
                    np.array([0, 0.5, 0.5]) * 100))

                assert False not in (np.ceil(each_stat.get_edge_list() * 100) == np.ceil(
                    [1.0, 3.0, 5.0, 7.0] * 100))
