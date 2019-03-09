from parallelm.mlops.stats.health.general_hist_stat import GeneralHistogramStat
from parallelm.mlops.stats.health.histogram_data_objects import ContinuousHistogramDataObject, \
    CategoricalHistogramDataObject


def test_general_stat_initialization():
    set_of_features_values = [[1, 2, 3], [2, 3, 4], [3, 3, 5]]

    set_of_features_names = ["c0", "c1", "c2"]

    model_stat_with_both = []

    general_histogram_stat_with_both = GeneralHistogramStat()
    general_histogram_stat_with_both.max_cat_unique_values = 3
    general_histogram_stat_with_both.create_and_set_general_stat(set_of_features_values=set_of_features_values,
                                                                 set_of_features_names=set_of_features_names,
                                                                 model_stat=model_stat_with_both)

    assert set(general_histogram_stat_with_both.set_of_continuous_features) == {"c0", "c2"}
    assert set(general_histogram_stat_with_both.set_of_categorical_features) == {"c1"}
    assert general_histogram_stat_with_both.contender_continuous_histogram == []
    assert general_histogram_stat_with_both.contender_categorical_histogram == []
    assert general_histogram_stat_with_both.contender_continuous_hist_bins is None

    model_stat_with_cont_stat = [
        u'{"data":"{\\"c2\\": [{\\"-inf to -2.1273143387113773\\": 0.0}, {\\"-2.1273143387113773 to -1.1041662771274905\\": 0.16666666666666666}, {\\"-1.1041662771274905 to inf\\": 0.0}], \\"c0\\": [{\\"-inf to -17.229967937210233\\": 0.0}, {\\"-17.229967937210233 to -12.15785255468716\\": 0.0}, {\\"-12.15785255468716 to inf\\": 0.16666666666666666}]}","graphType":"BARGRAPH","timestamp":1536614947113726976,"mode":"INSTANT","name":"continuousDataHistogram","type":"Health"}']
    general_histogram_stat_with_cont_stat = GeneralHistogramStat()
    general_histogram_stat_with_cont_stat.create_and_set_general_stat(set_of_features_values=set_of_features_values,
                                                                      set_of_features_names=set_of_features_names,
                                                                      model_stat=model_stat_with_cont_stat)

    assert set(general_histogram_stat_with_cont_stat.set_of_continuous_features) == {"c0", "c2"}
    assert general_histogram_stat_with_cont_stat.set_of_categorical_features == []

    for each_continuous_histogram_data_object in general_histogram_stat_with_cont_stat.contender_continuous_histogram:
        assert isinstance(each_continuous_histogram_data_object, ContinuousHistogramDataObject)
        if each_continuous_histogram_data_object.get_feature_name() == "c2":
            assert each_continuous_histogram_data_object.get_edges() == ['-inf to -2.1273143387113773',
                                                                         '-2.1273143387113773 to -1.1041662771274905',
                                                                         '-1.1041662771274905 to inf']
            assert each_continuous_histogram_data_object.get_bins() == [0.0, 0.16666666666666666, 0.0]

        elif each_continuous_histogram_data_object.get_feature_name() == "c0":
            assert each_continuous_histogram_data_object.get_edges() == ['-inf to -17.229967937210233',
                                                                         '-17.229967937210233 to -12.15785255468716',
                                                                         '-12.15785255468716 to inf']
            assert each_continuous_histogram_data_object.get_bins() == [0.0, 0.0, 0.16666666666666666]

    assert general_histogram_stat_with_cont_stat.contender_categorical_histogram == []

    model_stat_with_cat_stat = [
        u'{"data":"{\\"c2\\": [{\\"A\\": 0.0}, {\\"B\\": 0.16666666666666666}, {\\"C\\": 0.0}], \\"c0\\": [{\\"D\\": 0.0}, {\\"E\\": 0.0}, {\\"F\\": 0.16666666666666666}]}","graphType":"BARGRAPH","timestamp":1536614947113726976,"mode":"INSTANT","name":"categoricalDataHistogram","type":"Health"}']
    general_histogram_stat_with_cat_stat = GeneralHistogramStat()
    general_histogram_stat_with_cat_stat.create_and_set_general_stat(set_of_features_values=set_of_features_values,
                                                                     set_of_features_names=set_of_features_names,
                                                                     model_stat=model_stat_with_cat_stat)

    assert general_histogram_stat_with_cat_stat.set_of_continuous_features == []
    assert set(general_histogram_stat_with_cat_stat.set_of_categorical_features) == {"c0", "c2"}

    for each_categorical_histogram_data_object in general_histogram_stat_with_cat_stat.contender_categorical_histogram:
        assert isinstance(each_categorical_histogram_data_object, CategoricalHistogramDataObject)
        if each_categorical_histogram_data_object.get_feature_name() == "c2":
            assert each_categorical_histogram_data_object.get_edges() == ['A',
                                                                          'B',
                                                                          'C']
            assert list(each_categorical_histogram_data_object.get_bins()) == [0.0, 0.16666666666666666, 0.0]

        elif each_categorical_histogram_data_object.get_feature_name() == "c0":
            assert each_categorical_histogram_data_object.get_edges() == ['D',
                                                                          'E',
                                                                          'F']
            assert each_categorical_histogram_data_object.get_bins() == [0.0, 0.0, 0.16666666666666666]

    assert general_histogram_stat_with_cat_stat.contender_continuous_histogram == []

    model_stat_with_both_stat = [
        u'{"data":"{\\"c2\\": [{\\"A\\": 0.0}, {\\"B\\": 0.16666666666666666}, {\\"C\\": 0.0}]}","graphType":"BARGRAPH","timestamp":1536614947113726976,"mode":"INSTANT","name":"categoricalDataHistogram","type":"Health"}',
        u'{"data":"{\\"c0\\": [{\\"1 to 3\\": 0.0}, {\\"3 to 5\\": 0.1}, {\\"5 to 7\\": 0.0}]}","graphType":"BARGRAPH","timestamp":1536614947113726976,"mode":"INSTANT","name":"continuousDataHistogram","type":"Health"}'
    ]
    general_histogram_stat_with_both_stat = GeneralHistogramStat()
    general_histogram_stat_with_both_stat.create_and_set_general_stat(set_of_features_values=set_of_features_values,
                                                                      set_of_features_names=set_of_features_names,
                                                                      model_stat=model_stat_with_both_stat)

    assert set(general_histogram_stat_with_both_stat.set_of_continuous_features) == {"c0"}
    assert set(general_histogram_stat_with_both_stat.set_of_categorical_features) == {"c2"}

    for each_categorical_histogram_data_object in general_histogram_stat_with_both_stat.contender_categorical_histogram:
        assert isinstance(each_categorical_histogram_data_object, CategoricalHistogramDataObject)
        assert each_categorical_histogram_data_object.get_edges() == ['A',
                                                                      'B',
                                                                      'C']
        assert each_categorical_histogram_data_object.get_bins() == [0.0, 0.16666666666666666, 0.0]

    for each_categorical_histogram_data_object in general_histogram_stat_with_both_stat.contender_continuous_histogram:
        assert each_categorical_histogram_data_object.get_edges() == ['1 to 3',
                                                                      '3 to 5',
                                                                      '5 to 7']
        assert each_categorical_histogram_data_object.get_bins() == [0.0, 0.1, 0.0]


def test_comparator():
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

    features, score = GeneralHistogramStat.calculate_overlap_score(inferring_hist_rep=i_categorical_histogram,
                                                                   contender_hist_rep=c_categorical_histogram)

    assert set(features) == {"c1", "c2"}
    # score of c2
    assert int(score[0] * 100) == 83

    # score of c1
    assert int(score[1] * 100) == 83


def test_special_feature_handle_in_continuous_feature():
    set_of_features_values = [[1, 2, 3, "A", "1993/12/1"], [2, 3, 4, "B", "1993/12/1"], [3, 3, 5, "C", "1993/12/1"]]

    set_of_features_names = ["c0", "c1", "c2", "c3", "c4"]

    model_stat_with_both = []

    general_histogram_stat_with_both = GeneralHistogramStat()
    general_histogram_stat_with_both.max_cat_unique_values = 3
    general_histogram_stat_with_both.create_and_set_general_stat(set_of_features_values=set_of_features_values,
                                                                 set_of_features_names=set_of_features_names,
                                                                 model_stat=model_stat_with_both)

    # c3 is continuous since it has more than 3 values. but cannot be used at continuous as it is not numeric
    assert set(general_histogram_stat_with_both.set_of_continuous_features) == {"c0", "c2"}

    assert set(general_histogram_stat_with_both.set_of_categorical_features) == {"c1", "c4"}
    assert general_histogram_stat_with_both.contender_continuous_histogram == []
    assert general_histogram_stat_with_both.contender_categorical_histogram == []
    assert general_histogram_stat_with_both.contender_continuous_hist_bins is None
