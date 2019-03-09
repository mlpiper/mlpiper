import logging

import numpy as np

from parallelm.mlops.constants import PyHealth
from parallelm.mlops.data_analysis.categorical_data_analyst import CategoricalDataAnalyst
from parallelm.mlops.data_analysis.continuous_data_analyst import ContinuousDataAnalyst
from parallelm.mlops.stats.health.categorical_hist_stat import CategoricalHistogram
from parallelm.mlops.stats.health.continuous_hist_stat import ContinuousHistogram
from parallelm.mlops.stats.health.general_hist_stat import GeneralHistogramStat
from parallelm.mlops.stats.heatmap_stat import _HeatMapStat
from parallelm.mlops.stats.histogram_overlap_score_stat import _HistogramOverlapScoreStat
from parallelm.mlops.stats.histogram_stat import _HistogramStat
from parallelm.mlops.stats.table import Table
from parallelm.protobuf.ReflexEvent_pb2 import ReflexEvent


class PythonChannelHealth(object):
    """
    Class is responsible for generating health and heatmap given feature values, names and model_stat
    """

    @staticmethod
    def _create_feature_subset(features_values, features_names, selection_features_subset):
        """
        Feature selects features subset from values.
        For example, if user wants feature array of features - c0 and c2 from
        feature of c0--c10, then it will select column c0 and c2 and return np-array.
        """

        features_values = np.array(features_values)
        features_names = list(features_names)
        selection_features_subset = list(selection_features_subset)

        # feature values in order of names
        subset_feature_values_array = []
        for each_selection_f_names in selection_features_subset:
            subset_feature_values_array.append(
                features_values[:, features_names.index(each_selection_f_names)])
        return np.array(subset_feature_values_array).T

    @staticmethod
    def _create_current_hist_rep(features_values,
                                 features_names,
                                 num_bins,
                                 pred_bins_hist,
                                 stat_object_method,
                                 name_of_stat,
                                 model_id):
        """
        Method is responsible for creating  histogram from given values and names and return histogram lists.
        Type of histogram - continuous/categorical - depends on name_of_stat.
        :param features_values: feature array
        :param features_names: feature names
        :param num_bins: max number of bins for continuous features.
        :param pred_bins_hist: if bins are provided, to use as default.
        :param stat_object_method: stat object method to output stat
        :param name_of_stat:
        :param model_id: id of the mlobject stat is related to
        :return: list of  continuous histogram.
        """
        current_histogram_representation = None

        # generating histogram if features_values exists.
        if len(features_values) > 0:

            if name_of_stat is PyHealth.CONTINUOUS_HISTOGRAM_KEY:
                current_histogram = ContinuousHistogram() \
                    .fit(features_values,
                         features_names,
                         num_bins=num_bins,
                         pred_bins=pred_bins_hist)

            elif name_of_stat is PyHealth.CATEGORICAL_HISTOGRAM_KEY:
                current_histogram = CategoricalHistogram() \
                    .fit(features_values,
                         features_names,
                         num_bins=num_bins,
                         pred_bins=pred_bins_hist)
            else:
                raise Exception("_create_current_hist_rep is not compatible with: {}".format(name_of_stat))

            current_histogram_representation = \
                current_histogram.get_feature_histogram_rep()

            mlops_histogram_stat = _HistogramStat() \
                .name(name_of_stat)

            for each_features_rep in current_histogram_representation:
                mlops_histogram_stat.add_feature_data(feature=each_features_rep.get_feature_name(),
                                                      edge=each_features_rep.get_edges(),
                                                      bin=each_features_rep.get_bins())
            if stat_object_method is not None:
                stat_object_method(mlops_stat=mlops_histogram_stat.get_mlops_stat(model_id),
                                   reflex_event_message_type=ReflexEvent.MLHealthModel)

        return current_histogram_representation

    @staticmethod
    def _create_current_continuous_heatmap_rep(continuous_features_values,
                                               continuous_features_names,
                                               stat_object_method,
                                               model_id):
        """
        Method is responsible for creating heatmap outof given feature values and report it using stat_object_method

        :param continuous_features_values: feature array
        :param continuous_features_names: feature names
        :param stat_object_method: stat object method to output stat
        :return:
        """

        valid_feature_indexes = []
        valid_feature_values = []
        valid_feature_names = []
        non_valid_feature_indexes = []

        # convert whole nd array to float!
        for each_feature_index in range(continuous_features_values.shape[1]):
            try:

                valid_feature_values.append(np.array(continuous_features_values[:, each_feature_index], dtype=float))
                valid_feature_names.append(continuous_features_names[each_feature_index])
                valid_feature_indexes.append(each_feature_index)
            except:
                non_valid_feature_indexes.append(each_feature_index)

        valid_feature_values = np.array(valid_feature_values).T

        if len(non_valid_feature_indexes) > 0:
            logging.info("{} cannot be used for continuous heatmap creation."
                         .format(np.array(continuous_features_names)[non_valid_feature_indexes]))

        # creating heatmap -- min-max normalization and then taking mean.
        heat_map_values = []
        if len(valid_feature_values) > 0:

            max_continuous_features_values = np.max(valid_feature_values, axis=0)
            min_continuous_features_values = np.min(valid_feature_values, axis=0)

            for each_feature_index in range(len(valid_feature_names)):

                #  for constant values,
                # min value has to be 0
                # if max is also 0, then it needs to be changed to 1.0 to prevent 0/0
                if (max_continuous_features_values[each_feature_index]
                        == min_continuous_features_values[each_feature_index]):
                    min_continuous_features_values[each_feature_index] = 0.0
                    if max_continuous_features_values[each_feature_index] == 0.0:
                        max_continuous_features_values[each_feature_index] = 1.0

            max_min_diff = max_continuous_features_values - min_continuous_features_values

            normalized_continuous_features_values = (valid_feature_values -
                                                     min_continuous_features_values) * 1.0 / \
                                                    max_min_diff

            heat_map_values = np.mean(normalized_continuous_features_values, axis=0)

            mlops_heatmap_stat = _HeatMapStat() \
                .name(PyHealth.HEATMAP_KEY) \
                .features(list(valid_feature_names)) \
                .data(list(heat_map_values))

            if stat_object_method is not None:
                stat_object_method(mlops_stat=mlops_heatmap_stat.get_mlops_stat(model_id),
                                   reflex_event_message_type=ReflexEvent.StatsMessage)

        return valid_feature_names, heat_map_values

    @staticmethod
    def _compare_health(current_histogram_representation,
                        contender_histogram_representation,
                        stat_object_method,
                        name_of_stat,
                        model_id):
        """
        Method is responsible for comparing two histogram representation and output score using stat_object_method

        :param current_histogram_representation: inferring histogram representation
        :param contender_histogram_representation: training histogram representation
        :param stat_object_method: stat object method to output stat
        :param name_of_stat: be it continuous or categorical
        :return:
        """
        # If contender_histogram_representation is present then overlap can be calculated """

        contender_histogram_present = False
        if isinstance(contender_histogram_representation, list):
            if len(contender_histogram_representation) > 0:
                contender_histogram_present = True

        elif contender_histogram_representation is not None:
            contender_histogram_present = True

        compared_feature_names = []
        compared_feature_score = []
        if contender_histogram_present:
            compared_feature_names, compared_feature_score = \
                GeneralHistogramStat.calculate_overlap_score(
                    contender_hist_rep=contender_histogram_representation,
                    inferring_hist_rep=current_histogram_representation)

            mlops_stat_hist_score = _HistogramOverlapScoreStat() \
                .name(name_of_stat) \
                .features(list(compared_feature_names)) \
                .data(list(compared_feature_score))

            if stat_object_method is not None:
                stat_object_method(mlops_stat=mlops_stat_hist_score.get_mlops_stat(model_id),
                                   reflex_event_message_type=ReflexEvent.MLHealthModel)

        return compared_feature_names, compared_feature_score

    @staticmethod
    def generate_health_and_heatmap_stat(stat_object_method,
                                         logger,
                                         features_values,
                                         features_names,
                                         model_stat,
                                         model_id,
                                         num_bins=13,
                                         # TODO: Have ability to get this argument from user!
                                         data_analysis=True):
        """
        Method is highly responsible and creates continuous/categorical histograms. Also creates heatmap and compare two histogram if program is running on inference.

        :param stat_object_method: stat object method to output stat
        :param logger: logger to log
        :param features_values: feature array
        :param features_names: feature names
        :param model_stat: model stat
        :param num_bins: max number of bins for features.
        :return:
        """
        # generating general stats like categorical/continuous features and contender histograms.
        general_hist_stat = GeneralHistogramStat()
        general_hist_stat \
            .create_and_set_general_stat(set_of_features_values=features_values,
                                         set_of_features_names=features_names,
                                         model_stat=model_stat)

        # For Continuous Values
        # continuous feature names
        continuous_features_names = general_hist_stat.set_of_continuous_features
        # predefined bins of contender continuous hist
        pred_bins_continuous_hist = general_hist_stat.contender_continuous_hist_bins
        contender_continuous_histogram_representation = general_hist_stat.contender_continuous_histogram

        continuous_features_values = PythonChannelHealth. \
            _create_feature_subset(features_values=features_values,
                                   features_names=features_names,
                                   selection_features_subset=continuous_features_names)
        current_continuous_histogram_representation = \
            PythonChannelHealth._create_current_hist_rep(
                features_values=continuous_features_values,
                features_names=continuous_features_names,
                num_bins=num_bins,
                pred_bins_hist=pred_bins_continuous_hist,
                stat_object_method=stat_object_method,
                name_of_stat=PyHealth.CONTINUOUS_HISTOGRAM_KEY,
                model_id=model_id)

        # running data analysis for continuous dataset
        if data_analysis:
            continuous_data_analyst_result = ContinuousDataAnalyst \
                .analyze(set_of_continuous_feature_names=continuous_features_names,
                         set_of_continuous_feature_values=continuous_features_values)

            # outputting stat only if analysis result is there
            if len(continuous_data_analyst_result) > 0:
                cont_da = Table() \
                    .name("Continuous Data Analysis") \
                    .cols(["Count",
                           "Missing",
                           "Zeros",
                           "Standard Deviation",
                           "Min",
                           "Mean",
                           "Median",
                           "Max"])

                for f_n in continuous_data_analyst_result.keys():
                    f_v = continuous_data_analyst_result[f_n]
                    cont_da.add_row(str(f_v.feature_name),
                                    [f_v.count,
                                     f_v.NAs,
                                     f_v.zeros,
                                     f_v.std,
                                     f_v.min,
                                     f_v.mean,
                                     f_v.median,
                                     f_v.max])

                # outputting stat using stat object as stat message type
                stat_object_method(mlops_stat=cont_da.get_mlops_stat(model_id=model_id),
                                   reflex_event_message_type=ReflexEvent.StatsMessage)

        logger.debug("continuous features values: {}".format(continuous_features_values))
        logger.debug("continuous features names: {}".format(continuous_features_names))
        logger.debug(
            "current histogram representation: {}".format(current_continuous_histogram_representation))
        logger.debug(
            "contender histogram representation: {}".format(contender_continuous_histogram_representation))

        # For Categorical Values
        # categorical feature names
        categorical_features_names = general_hist_stat.set_of_categorical_features

        # predefined bins of contender categorical hist
        pred_bins_categorical_hist = general_hist_stat.contender_categorical_hist_bins
        contender_categorical_histogram_representation = general_hist_stat.contender_categorical_histogram

        categorical_features_values = PythonChannelHealth._create_feature_subset(features_values=features_values,
                                                                                 features_names=features_names,
                                                                                 selection_features_subset=categorical_features_names)
        current_categorical_histogram_representation = \
            PythonChannelHealth._create_current_hist_rep(
                categorical_features_values,
                categorical_features_names,
                num_bins,
                pred_bins_categorical_hist,
                stat_object_method,
                name_of_stat=PyHealth.CATEGORICAL_HISTOGRAM_KEY,
                model_id=model_id)

        # running data analysis for categorical dataset
        if data_analysis:
            categorical_data_analyst_result = CategoricalDataAnalyst \
                .analyze(set_of_categorical_feature_names=categorical_features_names,
                         set_of_categorical_feature_values=categorical_features_values)

            # outputting stat only if analysis result is there
            if len(categorical_data_analyst_result) > 0:

                categ_da = Table() \
                    .name("Categorical Data Analysis") \
                    .cols(["Count",
                           "Missing",
                           "Uniques",
                           "Top Frequently Occurring Category",
                           "Top Frequency",
                           "Average String Length"])

                for f_n in categorical_data_analyst_result.keys():
                    f_v = categorical_data_analyst_result[f_n]
                    categ_da. \
                        add_row(str(f_v.feature_name),
                                [f_v.count,
                                 f_v.NAs,
                                 f_v.unique,
                                 f_v.top,
                                 f_v.freq_top,
                                 f_v.avg_str_len])

                # outputting stat using stat object as stat message type
                stat_object_method(mlops_stat=categ_da.get_mlops_stat(model_id=model_id),
                                   reflex_event_message_type=ReflexEvent.StatsMessage)

        logger.debug("categorical features values: {}".format(categorical_features_values))
        logger.debug("categorical features names: {}".format(categorical_features_names))
        logger.debug(
            "current histogram representation: {}".format(current_categorical_histogram_representation))
        logger.debug(
            "contender histogram representation: {}".format(contender_categorical_histogram_representation))

        # If model_stat is given, it means it is inference program
        # so it needs to create heatmap and score too.
        if model_stat is not None:
            if continuous_features_values.shape[0] > 0:
                continuous_features_names, heat_map_values = PythonChannelHealth. \
                _create_current_continuous_heatmap_rep(continuous_features_values=continuous_features_values,
                                                       continuous_features_names=continuous_features_names,
                                                       stat_object_method=stat_object_method,
                                                       model_id=model_id)
                logger.debug("features: {}, heatmap values: {}".format(continuous_features_names,
                                                                   heat_map_values))

                compared_continuous_feature_names, compared_continuous_feature_score = PythonChannelHealth. \
                    _compare_health(
                        current_histogram_representation=current_continuous_histogram_representation,
                        contender_histogram_representation=contender_continuous_histogram_representation,
                        stat_object_method=stat_object_method,
                        name_of_stat=PyHealth.CONTINUOUS_HISTOGRAM_OVERLAP_SCORE_KEY,
                        model_id=model_id)
                logger.debug(
                    "continuous features: {}, overlap scores: {}".format(compared_continuous_feature_names,
                                                                     compared_continuous_feature_score))

            if categorical_features_values.shape[0] > 0:
                compared_categorical_feature_names, compared_categorical_feature_names = PythonChannelHealth. \
                    _compare_health(
                        current_histogram_representation=current_categorical_histogram_representation,
                        contender_histogram_representation=contender_categorical_histogram_representation,
                        stat_object_method=stat_object_method,
                        name_of_stat=PyHealth.CATEGORICAL_HISTOGRAM_OVERLAP_SCORE_KEY,
                        model_id=model_id)
                logger.debug(
                    "categorical features: {}, overlap scores: {}".format(
                     compared_categorical_feature_names, compared_categorical_feature_names))
