import numpy as np

from parallelm.mlops.channels.python_channel_health import PythonChannelHealth
from parallelm.mlops.constants import PyHealth
from parallelm.mlops.stats.health.histogram_data_objects import ContinuousHistogramDataObject, \
    CategoricalHistogramDataObject


def test_selection_of_features():
    features_values = np.array(
        [[1, 2, 3, "A"], [2, 3, 4, "B"], [2, 3, 4, "A"], [3, 4, 4, "B"], [2, 3, 4, "B"], [1, 24, 34, "B"],
         [1, 24, 34, "B"], [1, 24, 34, "B"]])
    feature_length = len(features_values[0])

    features_names = []
    for i in range(feature_length):
        features_names.append("c" + str(i))

    selection_features_subset = ["c0", "c3"]

    expect_subset_feature_values = np.array([['1', 'A'],
                                             ['2', 'B'],
                                             ['2', 'A'],
                                             ['3', 'B'],
                                             ['2', 'B'],
                                             ['1', 'B'],
                                             ['1', 'B'],
                                             ['1', 'B']])

    assert False not in (PythonChannelHealth._create_feature_subset(features_values, features_names,
                                                                    selection_features_subset)
                         ==
                         expect_subset_feature_values)


def test_creation_of_histogram():
    features_values = np.array([[1, 2, 3], [2, 3, 4]])
    features_names = ["A", "B", "C"]
    cont_hist_rep = PythonChannelHealth._create_current_hist_rep(features_values,
                                                                 features_names,
                                                                 num_bins=3,
                                                                 pred_bins_hist=None,
                                                                 stat_object_method=None,
                                                                 name_of_stat=PyHealth.CONTINUOUS_HISTOGRAM_KEY,
                                                                 model_id="model_id")

    a_cont_hist = cont_hist_rep[0]
    assert isinstance(a_cont_hist, ContinuousHistogramDataObject)
    assert a_cont_hist.get_feature_name() == "A"
    assert False not in (a_cont_hist.get_bins() == [0, 1, 0])
    assert a_cont_hist.get_edges() == ['-inf to 0.5', '0.5 to 2.5', '2.5 to inf']

    b_cont_hist = cont_hist_rep[1]
    assert isinstance(b_cont_hist, ContinuousHistogramDataObject)
    assert b_cont_hist.get_feature_name() == "B"
    assert False not in (b_cont_hist.get_bins() == [0, 1, 0])
    assert b_cont_hist.get_edges() == ['-inf to 1.5', '1.5 to 3.5', '3.5 to inf']

    c_cont_hist = cont_hist_rep[2]
    assert isinstance(c_cont_hist, ContinuousHistogramDataObject)
    assert c_cont_hist.get_feature_name() == "C"
    assert False not in (c_cont_hist.get_bins() == [0, 1, 0])
    assert c_cont_hist.get_edges() == ['-inf to 2.5', '2.5 to 4.5', '4.5 to inf']

    cat_hist_rep = PythonChannelHealth._create_current_hist_rep(features_values,
                                                                features_names,
                                                                num_bins=3,
                                                                pred_bins_hist=None,
                                                                stat_object_method=None,
                                                                name_of_stat=PyHealth.CATEGORICAL_HISTOGRAM_KEY,
                                                                model_id="model_id")

    a_cat_hist = cat_hist_rep[0]
    assert isinstance(a_cat_hist, CategoricalHistogramDataObject)
    assert a_cat_hist.get_feature_name() == "A"
    assert False not in (a_cat_hist.get_bins() == [0.5, 0.5])
    assert a_cat_hist.get_edges() == ['1', '2']

    b_cat_hist = cat_hist_rep[1]
    assert isinstance(b_cat_hist, CategoricalHistogramDataObject)
    assert b_cat_hist.get_feature_name() == "B"
    assert False not in (b_cat_hist.get_bins() == [0.5, 0.5])
    assert b_cat_hist.get_edges() == ['2', '3']

    c_cat_hist = cat_hist_rep[2]
    assert isinstance(c_cat_hist, CategoricalHistogramDataObject)
    assert c_cat_hist.get_feature_name() == "C"
    assert False not in (c_cat_hist.get_bins() == [0.5, 0.5])
    assert c_cat_hist.get_edges() == ['3', '4']


def test_creation_of_heatmap():
    continuous_features_values = np.array([[1, 10], [2, 100], [0, 100], [4, -10], [5, 50]])
    continuous_features_names = ["A", "B"]
    features_names, heatmap = PythonChannelHealth. \
        _create_current_continuous_heatmap_rep(continuous_features_values,
                                               continuous_features_names,
                                               stat_object_method=None,
                                               model_id=None)
    assert features_names == ["A", "B"]
    assert False not in (np.ceil(heatmap * 1000) == [481, 546])

    continuous_features_values_constant_A = np.array([[1, 10], [1, 100], [1, 100], [1, -10], [1, 50]])
    features_names, heatmap_constant_A = PythonChannelHealth. \
        _create_current_continuous_heatmap_rep(continuous_features_values_constant_A,
                                               continuous_features_names,
                                               stat_object_method=None,
                                               model_id=None)

    assert features_names == ["A", "B"]
    assert False not in (np.ceil(heatmap_constant_A * 1000) == [1000, 546])

    continuous_features_values_constant_B_with_zeros = np.array([[1, 0], [1, 0], [1, 0], [1, 0], [1, 0]])
    features_names, heatmap_constant_B_with_zeros = PythonChannelHealth. \
        _create_current_continuous_heatmap_rep(continuous_features_values_constant_B_with_zeros,
                                               continuous_features_names,
                                               stat_object_method=None,
                                               model_id=None)

    assert features_names == ["A", "B"]
    assert False not in (np.ceil(heatmap_constant_B_with_zeros * 1000) == [1000, 0])


def test_creation_of_heatmap_of_NaN():
    continuous_features_values = np.array([[1, np.NaN], [np.NaN, 100], [0, 100], [4, -10], [np.NaN, 50]])
    continuous_features_names = ["A", "B"]
    features_names, heatmap = PythonChannelHealth. \
        _create_current_continuous_heatmap_rep(continuous_features_values,
                                               continuous_features_names,
                                               stat_object_method=None,
                                               model_id=None)

    assert features_names == ["A", "B"]
    # For A ==> min = 0. max = 4. ==> so normalized A = [0.25, NaN, 0, 1, Nan] ==> mean = 0.417
    # For B ==> min = -10. max = 100. ==> so normalized B = [NaN, 1, 1, 0, 0.5454] ==> mean = 0.6363
    assert False not in (np.ceil(heatmap * 1000) == [417, 637])


def test_compare_health_functionality():
    i1_categorical_histogram_data_object = CategoricalHistogramDataObject(feature_name="c1", edges=["a", "b", "c", "d"],
                                                                          bins=[0.4, 0.2, 0.3, 0.1])
    c1_categorical_histogram_data_object = CategoricalHistogramDataObject(feature_name="c1", edges=["a", "b", "c", "d"],
                                                                          bins=[0.3, 0.4, 0.1, 0.2])
    i2_categorical_histogram_data_object = CategoricalHistogramDataObject(feature_name="c2", edges=["a", "b", "c"],
                                                                          bins=[0.4, 0.2, 0.3])
    c2_categorical_histogram_data_object = CategoricalHistogramDataObject(feature_name="c2", edges=["a", "b", "c"],
                                                                          bins=[0.3, 0.4, 0.1])
    i_categorical_histogram = [i1_categorical_histogram_data_object, i2_categorical_histogram_data_object]
    c_categorical_histogram = [c1_categorical_histogram_data_object, c2_categorical_histogram_data_object]

    features_names, health_score = PythonChannelHealth._compare_health(
        current_histogram_representation=i_categorical_histogram,
        contender_histogram_representation=c_categorical_histogram,
        stat_object_method=None,
        name_of_stat=PyHealth.CATEGORICAL_HISTOGRAM_OVERLAP_SCORE_KEY,
        model_id=None)

    assert set(features_names) == {"c1", "c2"}
    # score of c2
    assert int(health_score[0] * 100) == 83

    # score of c1
    assert int(health_score[1] * 100) == 83

    i1_continuous_histogram_data_object = ContinuousHistogramDataObject(feature_name="c1",
                                                                        edges=["a", "b", "c", "d"],
                                                                        bins=[0.4, 0.2, 0.3, 0.1])
    c1_continuous_histogram_data_object = ContinuousHistogramDataObject(feature_name="c1",
                                                                        edges=["a", "b", "c", "d"],
                                                                        bins=[0.3, 0.4, 0.1, 0.2])
    i2_continuous_histogram_data_object = ContinuousHistogramDataObject(feature_name="c2", edges=["a", "b", "c"],
                                                                        bins=[0.4, 0.2, 0.3])
    c2_continuous_histogram_data_object = ContinuousHistogramDataObject(feature_name="c2", edges=["a", "b", "c"],
                                                                        bins=[0.3, 0.4, 0.1])
    i_continuous_histogram = [i1_continuous_histogram_data_object, i2_continuous_histogram_data_object]
    c_continuous_histogram = [c1_continuous_histogram_data_object, c2_continuous_histogram_data_object]

    features_names, health_score = PythonChannelHealth._compare_health(
        current_histogram_representation=i_continuous_histogram,
        contender_histogram_representation=c_continuous_histogram,
        stat_object_method=None,
        name_of_stat=PyHealth.CONTINUOUS_HISTOGRAM_OVERLAP_SCORE_KEY,
        model_id=None)

    assert set(features_names) == {"c1", "c2"}
    # score of c2
    assert int(health_score[0] * 100) == 83

    # score of c1
    assert int(health_score[1] * 100) == 83
