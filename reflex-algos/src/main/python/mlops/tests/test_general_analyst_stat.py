import numpy as np
import pandas as pd

from parallelm.mlops.channels.python_channel_health import PythonChannelHealth
from parallelm.mlops.data_analysis.categorical_data_analyst import CategoricalDataAnalyst, CategoricalDataAnalysisResult
from parallelm.mlops.data_analysis.continuous_data_analyst import ContinuousDataAnalyst, ContinuousDataAnalysisResult
from parallelm.mlops.stats.health.general_hist_stat import GeneralHistogramStat


def test_continuous_feature_analysis():
    set_of_features_values = np.array([[1, 2, 3, "A", "1993/12/1"],
                                       [2, 1, np.NAN, "B", "1993/12/1"],
                                       [2, 3, 4, "B", "1993/12/1"],
                                       [0, 9, 5, "A", "1993/12/1"],
                                       [0, 9, 5, "A", np.nan],
                                       [2, 1, np.NAN, "B", "1993/12/1"]])

    set_of_features_names = ["c0", "c1", "c2", "c3", "c4"]

    model_stat_with_both = []

    general_histogram_stat_with_both = GeneralHistogramStat()
    general_histogram_stat_with_both.max_cat_unique_values = 3
    general_histogram_stat_with_both.create_and_set_general_stat(set_of_features_values=set_of_features_values,
                                                                 set_of_features_names=set_of_features_names,
                                                                 model_stat=model_stat_with_both)

    # c3 is continuous since it has more than 3 values. but cannot be used at continuous as it is not numeric
    assert set(general_histogram_stat_with_both.set_of_continuous_features) == {"c0", "c1", "c2"}
    assert set(general_histogram_stat_with_both.set_of_categorical_features) == {"c3", "c4"}

    # For Continuous Values
    # continuous feature names
    continuous_features_names = general_histogram_stat_with_both.set_of_continuous_features

    continuous_features_values = PythonChannelHealth._create_feature_subset(features_values=set_of_features_values,
                                                                            features_names=set_of_features_names,
                                                                            selection_features_subset=continuous_features_names)

    continuous_data_analyst_result = ContinuousDataAnalyst \
        .analyze(set_of_continuous_feature_names=continuous_features_names,
                 set_of_continuous_feature_values=continuous_features_values)

    assert len(continuous_data_analyst_result) == 3

    c2_feature_analysis = ContinuousDataAnalysisResult(
        feature_name="c2",
        count=6,
        NAs="33.33%",
        min=3.0,
        max=5.0,
        mean=4.25,
        median=4.5,
        std=0.9574,
        zeros=0)
    assert (continuous_data_analyst_result['c2'] == c2_feature_analysis)

    c1_feature_analysis = ContinuousDataAnalysisResult(
        feature_name="c1",
        count=6,
        NAs="0.0%",
        min=1.0,
        max=9.0,
        mean=4.1667,
        median=2.5,
        std=3.8166,
        zeros=0)
    assert (continuous_data_analyst_result['c1'] == c1_feature_analysis)

    c0_feature_analysis = ContinuousDataAnalysisResult(
        feature_name="c0",
        count=6,
        NAs="0.0%",
        min=0.0,
        max=2.0,
        mean=1.1667,
        median=1.5,
        std=0.9831,
        zeros=2)
    assert (continuous_data_analyst_result['c0'] == c0_feature_analysis)


def test_categorical_feature_analysis():
    raw_cat = pd.Categorical(["a", "b", "c", "a", "d", "b"], categories=["b", "c", "d"],
                             ordered=False)

    df = pd.DataFrame({"A": ["Harshil", "Yakov", "Sriram", "Sindhu", "LiorK", "Harshil"]})
    df["B"] = raw_cat

    set_of_features_values = df.values
    set_of_features_names = df.keys()

    model_stat_with_both = []

    general_histogram_stat_with_both = GeneralHistogramStat()
    general_histogram_stat_with_both.create_and_set_general_stat(set_of_features_values=set_of_features_values,
                                                                 set_of_features_names=set_of_features_names,
                                                                 model_stat=model_stat_with_both)

    categorical_features_names = general_histogram_stat_with_both.set_of_categorical_features

    categorical_features_values = PythonChannelHealth._create_feature_subset(features_values=set_of_features_values,
                                                                             features_names=set_of_features_names,
                                                                             selection_features_subset=categorical_features_names)

    categorical_data_analyst_result = CategoricalDataAnalyst \
        .analyze(set_of_categorical_feature_names=categorical_features_names,
                 set_of_categorical_feature_values=categorical_features_values)

    assert len(categorical_data_analyst_result) == 2

    A_feature_analysis = CategoricalDataAnalysisResult(
        feature_name="A",
        count=6,
        NAs="0.0%",
        unique=5,
        top="Harshil",
        freq_top=2,
        avg_str_len=6.0)
    assert (categorical_data_analyst_result["A"] == A_feature_analysis)

    B_feature_analysis = CategoricalDataAnalysisResult(
        feature_name="B",
        count=6,
        NAs="33.33%",
        unique=3,
        top="b",
        freq_top=2,
        avg_str_len=1.0)
    assert (categorical_data_analyst_result["B"] == B_feature_analysis)
