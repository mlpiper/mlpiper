"""
The Code contains functions to calculate univariate statistics for continuous features, given a dataset.

"""
import logging

import numpy as np

from parallelm.mlops.stats.health.histogram_data_objects import ContinuousHistogramDataObject


class ContinuousHistogram(object):
    """
    Class is responsible for providing fit and get_feature_histogram_rep functionality of continuous training dataset.
    """

    def __init__(self):
        """
        Class constructor to init variables.
        :rtype: object
        """
        # holds array of tuple of bin and edges. order can be followed by features list.
        self._prob_dist_continuous = []

        self._features = []

    def fit(self, training_feature_values, training_feature_names, num_bins, pred_bins):
        """
        Function is responsible for fitting training data and fill up _prob_dist_continuous containing tuples of bin and edges.
        :rtype: self
        """
        if isinstance(training_feature_values, np.ndarray):

            prob_dist, valid_feature_names = \
                self._cal_hist_params(training_feature_values=training_feature_values,
                                      training_feature_names=training_feature_names,
                                      num_bins=num_bins,
                                      pred_bins=pred_bins)
            self._prob_dist_continuous = prob_dist
            self._features = valid_feature_names

        else:
            raise Exception("continuous histograms are generated on numpy array only!")

        return self

    def get_feature_histogram_rep(self):
        """
        Function is responsible for creating formatted representation of continuous histogram. It will be used in forwarding stat through MLOps.
        :rtype: list containing ContinuousHistogramDataObject
        """
        feature_histogram_rep = []

        for index in range(len(self._features)):
            edges = self._prob_dist_continuous[index][0]
            bins = self._prob_dist_continuous[index][1]

            normalized_bins = bins / np.sum(bins)
            edges_rep = []
            for each_edge_index in range(0, len(edges) - 1):
                edges_rep.append(str(edges[each_edge_index]) + " to " + str(edges[each_edge_index + 1]))

            cont_histogram_data_object = ContinuousHistogramDataObject(feature_name=self._features[index],
                                                                       edges=edges_rep,
                                                                       bins=normalized_bins)
            feature_histogram_rep.append(cont_histogram_data_object)
        return feature_histogram_rep

    @staticmethod
    def _cal_hist_params(training_feature_values,
                         training_feature_names,
                         num_bins,
                         pred_bins=None):
        """
        Create a fixed number of bins and corresponding normalized frequency for a given dataset

        :param training_feature_values: A dataset that is a 2D numpy array
        :param num_bins: Number of bins to create
        :param pred_bins: pre-defined bins.
        :rtype: prob_dist: A list containing the frequency/probability distribution of values in each bin for the dataset.
                           Order of arrays in the list is same as the order of columns in sample data
        :rtype: valid_feature_names: Feature name list for which hist is generated
        """

        valid_feature_indexes = []
        valid_feature_values = []
        valid_feature_names = []
        non_valid_feature_indexes = []

        # convert whole nd array to float!
        for each_feature_index in range(training_feature_values.shape[1]):
            try:

                valid_feature_values.append(np.array(training_feature_values[:, each_feature_index], dtype=float))
                valid_feature_names.append(training_feature_names[each_feature_index])
                valid_feature_indexes.append(each_feature_index)
            except:
                non_valid_feature_indexes.append(each_feature_index)

        valid_feature_values = np.array(valid_feature_values).T

        if len(non_valid_feature_indexes) > 0:
            logging.info("{} cannot be used for continuous histogram creation."
                         .format(np.array(training_feature_names)[non_valid_feature_indexes]))

        # pred_bins = None
        if pred_bins is None or pred_bins is []:

            # Calculate the mean and std of the dataset
            mean = np.nanmean(valid_feature_values, axis=0)
            standard_deviation = np.nanstd(valid_feature_values, axis=0)

            logging.debug("mean {}\nstdev {}".format(mean, standard_deviation))
            # Bin values per feature, that include +inf and -inf same as our system
            bins = np.zeros((valid_feature_values.shape[1], num_bins + 1))
            bins_subset = np.zeros((valid_feature_values.shape[1], num_bins - 2))
            for a in range(0, valid_feature_values.shape[1]):
                # right now, constant bins or very small std will result in wrong bin creations.
                # So for that it will be 13 also with range of mean - 0.01 to mean + 0.01.
                # reason is, bins is matrix and it is not flexible to hold variable length vectors. It needs refactoring but we will come later.
                # TODO: https://parallelmachines.atlassian.net/browse/REF-4395
                logging.debug("feature: {}".format(valid_feature_names[a]))
                if standard_deviation[a] == 0.0:
                    standard_deviation[a] = 0.05
                bins_subset[a, :] = (np.arange(mean[a] - 2 * standard_deviation[a],
                                               mean[a] + 2 * standard_deviation[a],
                                               (4 * standard_deviation[a]) / (num_bins - 2)))[0:num_bins - 2]
            bins[:, 1:num_bins - 1] = bins_subset
            bins[:, 0] = -np.inf
            bins[:, num_bins - 1] = mean + 2 * standard_deviation
            bins[:, num_bins] = np.inf
        else:
            bins = []
            for each_valid_feature_indexes in valid_feature_indexes:
                bins.append(pred_bins[each_valid_feature_indexes])

            bins = np.array(bins)

        prob_dist = []
        for a in range(0, valid_feature_values.shape[1]):
            # Determine the frequency of unique values
            nan_array = valid_feature_values[:,a]
            no_nan_array = nan_array[~np.isnan(nan_array)]
            counts, bins_feature = np.histogram(no_nan_array, bins[a, :])

            prob_dist.append(np.asarray((bins_feature, counts * 1.0)).T)
        return prob_dist, valid_feature_names
