import json
import logging

import numpy as np

from parallelm.mlops.constants import PyHealth
from parallelm.mlops.stats.health.histogram_data_objects import ContinuousHistogramDataObject, \
    CategoricalHistogramDataObject
from parallelm.mlops.stats.stats_utils import check_hist_compare_requirement


class GeneralHistogramStat(object):
    """
    Class is responsible for providing general functionality like detecting categorical/continuous features or overlap scores.
    """
    # variable holds contender(training) continuous histogram
    contender_continuous_histogram = None
    # variable holds contender(training) continuous histogram bins.
    # It will be used by inference histogram creation so that inference histogram stays withing
    # bin range
    contender_continuous_hist_bins = None
    # variable holds list of continuous features.
    set_of_continuous_features = None

    # variable holds contender(training) categorical histogram
    contender_categorical_histogram = None
    # variable holds contender(training) categorical histogram bins. It will be used by inference
    # histogram creation so that inference histogram stays withing bin range
    contender_categorical_hist_bins = None
    # variable holds list of categorical features.
    set_of_categorical_features = None

    # variable is responsible for maximum unique value feature can hold for it to be categorical.
    max_cat_unique_values = PyHealth.MAXIMUM_CATEGORY_UNIQUE_VALUE_REQ

    def __init__(self):
        """
        Class constructor to init variables.
        :rtype: object
        """
        self.set_of_continuous_features = []
        self.set_of_categorical_features = []

        self.contender_continuous_histogram = []
        self.contender_categorical_histogram = []

    def create_and_set_general_stat(self, set_of_features_values, set_of_features_names, model_stat):
        """
        Set continuous/categorical features and contender histogram stats.

        :param set_of_features_values: set of feature values. It expects to be ndarray-2D
        :param set_of_features_names: set of feature names. It expects to be 1D array
        :param model_stat: If program is inference, then it may be possible to provide several model stat coming from training algorithm
        """

        if not isinstance(set_of_features_values, np.ndarray):
            set_of_features_values = np.array(set_of_features_values)

        # first case is if model stat is given. Then continuous/categorical features and contender histogram stats will be created from model stat only!
        if model_stat is not None and len(model_stat) > 0:

            not_found_features = []

            for each_model_stat in model_stat:
                accum_data = json.loads(each_model_stat)
                # it can be continuous histogram or categorical histogram
                if accum_data['name'] == PyHealth.CONTINUOUS_HISTOGRAM_KEY:
                    # setting continuous feature set and hist_bin range
                    continuous_feature_names_array = []
                    contender_continuous_hist_bins_array = []

                    cont_hist_rep = json.loads(accum_data['data'])

                    for feature_name in cont_hist_rep:
                        if feature_name in set_of_features_names:
                            edge_rep = []
                            bin_rep = []

                            for each_tuple_of_edge_bin in cont_hist_rep[feature_name]:
                                for each_edge in each_tuple_of_edge_bin:
                                    edge_rep.append(str(each_edge))
                                    bin_rep.append(each_tuple_of_edge_bin[each_edge])

                            continuous_hist_data_obj = ContinuousHistogramDataObject(feature_name=feature_name,
                                                                                     edges=edge_rep,
                                                                                     bins=bin_rep)

                            contender_continuous_hist_bins_array.append(continuous_hist_data_obj.get_edge_list())
                            continuous_feature_names_array.append(str(feature_name))
                            self.contender_continuous_histogram.append(continuous_hist_data_obj)

                        else:
                            not_found_features.append(feature_name)

                    self.set_of_continuous_features = np.array(continuous_feature_names_array)

                    self.contender_continuous_hist_bins = np.array(contender_continuous_hist_bins_array)

                elif accum_data['name'] == PyHealth.CATEGORICAL_HISTOGRAM_KEY:
                    # setting categorical feature set and hist_bin range
                    # setting categorical feature set
                    categorical_feature_names_array = []
                    contender_categorical_hist_bins_array = []

                    categorical_hist_rep = json.loads(accum_data['data'])
                    for feature_name in categorical_hist_rep:
                        if feature_name in set_of_features_names:
                            edge_rep = []
                            bin_rep = []

                            for each_tuple_of_edge_bin in categorical_hist_rep[feature_name]:
                                for each_edge in each_tuple_of_edge_bin:
                                    edge_rep.append(str(each_edge))
                                    bin_rep.append(each_tuple_of_edge_bin[each_edge])

                            categorical_hist_data_obj = CategoricalHistogramDataObject(feature_name=feature_name,
                                                                                       edges=edge_rep,
                                                                                       bins=bin_rep)

                            contender_categorical_hist_bins_array.append(categorical_hist_data_obj.get_edge_list())
                            categorical_feature_names_array.append(str(feature_name))

                            self.contender_categorical_histogram.append(categorical_hist_data_obj)
                        else:
                            not_found_features.append(feature_name)
                    self.set_of_categorical_features = np.array(categorical_feature_names_array)

                    self.contender_categorical_hist_bins = np.array(contender_categorical_hist_bins_array)

            if len(not_found_features):
                logging.info("{} features are not found in set_of_features_names.".format(not_found_features))

        # If model_stat is not given, then it has to be decided by max unique values
        else:
            for each_feature_index in range(0, set_of_features_values.shape[1]):
                feature_value = set_of_features_values[:, each_feature_index]

                # isnan can only be performed on numpy types - so attempt to convert to float -
                # if that fails, just use the full array
                try:
                    no_nans = feature_value[~np.isnan(feature_value)]
                except:
                    try:
                        float_fv = feature_value.astype(float)
                        no_nans = feature_value[~np.isnan(float_fv)]
                    except:
                        no_nans = feature_value

                likely_cat = len(set(no_nans)) < self.max_cat_unique_values

                if len(no_nans) == 0 or likely_cat == 1:
                    logging.info("{} is categorical".format(
                        set_of_features_names[each_feature_index]))
                    self.set_of_categorical_features.append(
                        set_of_features_names[each_feature_index])
                else:
                    try:
                        # check if the data represents a continuous value
                        logging.info("{} is continuous".format(
                            set_of_features_names[each_feature_index]))
                        set_of_features_values[:, each_feature_index] =\
                            np.array(set_of_features_values[:, each_feature_index], dtype=float)
                        # TODO: convert to numpy instead of array
                        self.set_of_continuous_features.append(
                            set_of_features_names[each_feature_index])
                    except Exception as e:
                        logging.warning("{} feature cannot be converted to continuous data".format(
                            set_of_features_names[each_feature_index]))

    @staticmethod
    def calculate_overlap_score(inferring_hist_rep, contender_hist_rep):
        """
        Calculate overlap score for given list of featured histograms.
        :param inferring_hist_rep: inference histogram list containing either ContinuousHistogramDataObject or CategoricalHistogramDataObject
        :param contender_hist_rep: contender histogram list containing either ContinuousHistogramDataObject or CategoricalHistogramDataObject
        """
        check_hist_compare_requirement(inferring_hist_rep=inferring_hist_rep,
                                       contender_hist_rep=contender_hist_rep)

        # dict holds, dict of features and values will be dict of bins and edges
        inference_feature_edges_bins = {}
        contender_feature_edges_bins = {}

        for each_inferring_hist_rep in inferring_hist_rep:

            bins = list(each_inferring_hist_rep.get_bins())
            edges = list(each_inferring_hist_rep.get_edges())
            dict_of_edges_bins = {}

            for each_index_of_hist_edge_bin in range(len(bins)):
                dict_of_edges_bins[edges[each_index_of_hist_edge_bin]] = bins[each_index_of_hist_edge_bin]

            inference_feature_edges_bins[each_inferring_hist_rep.get_feature_name()] = dict_of_edges_bins

        for each_contender_hist_rep in contender_hist_rep:

            bins = list(each_contender_hist_rep.get_bins())
            edges = list(each_contender_hist_rep.get_edges())
            dict_of_edges_bins = {}

            for each_index_of_hist_edge_bin in range(len(bins)):
                dict_of_edges_bins[edges[each_index_of_hist_edge_bin]] = bins[each_index_of_hist_edge_bin]

            contender_feature_edges_bins[each_contender_hist_rep.get_feature_name()] = dict_of_edges_bins

        features_name = []
        overlap_scores = []

        # handling case where training identified one feature as continuous but it is missing in inference.
        intersect_features = \
            set(contender_feature_edges_bins.keys()) \
                .intersection(set(inference_feature_edges_bins.keys()))

        for each_feature_key_object in intersect_features:
            string_feature = str(each_feature_key_object)
            features_name.append(string_feature)
            score = GeneralHistogramStat._compare_hist_using_probability_distribution(
                inferring_hist_cats=inference_feature_edges_bins[string_feature],
                contender_hist_cats=contender_feature_edges_bins[string_feature])
            overlap_scores.append(score)

        return features_name, overlap_scores

    # Comparision formula is as following.
    #          square of(sum of contender histogram counts)      sum(contender histogram count * inference histogram count)
    # score = -------------------------------------------- * -----------------------------------------------------------------
    #           sum(square of contender histogram counts)     sum(contender histogram counts) * sum(inference histogram counts)

    @staticmethod
    def _compare_hist_using_probability_distribution(inferring_hist_cats, contender_hist_cats):
        """
        calculate overlap score for given dict of bin and edges
        :param inferring_hist_cats: inference dict of bin to edges.
        :param contender_hist_cats: contender dict of bin to edges.
        """
        if not (isinstance(inferring_hist_cats, dict)):
            raise Exception("two objects cannot be compared as inferring_hist_cats is not dict type")

        if not isinstance(contender_hist_cats, dict):
            raise Exception("two objects cannot be compared as contender_hist_cats is not dict type")

        score = 0.0
        # variable will hold sum of multiplication of both histogram counts
        sum_of_mult_of_both_hist_count = 0.0
        # variable will hold sum of square of referenced histogram counts
        sum_of_square_of_contender_hist_count = 0.0
        # variable will hold sum of referenced histogram counts
        sum_of_contender_hist_count = 0.0
        # variable will hold sum of contender histogram counts
        sum_of_inferenced_hist_count = 0.0

        total_categories = list(set(inferring_hist_cats.keys()) | set(contender_hist_cats.keys()))

        for each_cat in total_categories:
            contender_hist_count = contender_hist_cats.get(each_cat, 0.0)
            inf_hist_count = inferring_hist_cats.get(each_cat, 0.0)

            sum_of_contender_hist_count += contender_hist_count
            sum_of_inferenced_hist_count += inf_hist_count

            sum_of_mult_of_both_hist_count += contender_hist_count * inf_hist_count
            sum_of_square_of_contender_hist_count += contender_hist_count * contender_hist_count

        # This block will be responsible for calculation the score for remaining categories which could not included in histograms
        # Maximum sum of count for all normalized category count is 1.

        if sum_of_contender_hist_count <= 1.0 and sum_of_inferenced_hist_count <= 1.0:
            max_normalized_categories_count = 1.0
            remaining_contender_hist_count = max_normalized_categories_count - sum_of_contender_hist_count
            remaining_inferenced_hist_count = max_normalized_categories_count - sum_of_inferenced_hist_count

            sum_of_contender_hist_count = max_normalized_categories_count
            sum_of_inferenced_hist_count = max_normalized_categories_count

            sum_of_mult_of_both_hist_count += remaining_contender_hist_count * remaining_inferenced_hist_count
            sum_of_square_of_contender_hist_count += remaining_contender_hist_count * remaining_contender_hist_count

        if sum_of_square_of_contender_hist_count != 0 and sum_of_contender_hist_count != 0 and sum_of_inferenced_hist_count != 0:
            contender_hist_reference_value = (sum_of_contender_hist_count * sum_of_contender_hist_count) \
                                             / sum_of_square_of_contender_hist_count
            inference_hist_reference_value = sum_of_mult_of_both_hist_count / \
                                             (sum_of_contender_hist_count * sum_of_inferenced_hist_count)

            score = contender_hist_reference_value * inference_hist_reference_value

        # clipping score if it goes beyond 1.0

        if score > 1.0:
            score = 1.0

        return score
