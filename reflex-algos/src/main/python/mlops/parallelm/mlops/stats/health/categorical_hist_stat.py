"""
The Code contains functions to calculate univariate statistics for categorical features, given a dataset.

"""

import numpy as np

from parallelm.mlops.stats.health.histogram_data_objects import CategoricalHistogramDataObject


class CategoricalHistogram(object):
    """
    Class is responsible for providing fit and get_feature_histogram_rep functionality of categorical training dataset.
    """

    def __init__(self):
        """
        Class constructor to init variables.
        :rtype: object
        """
        # holds array of tuple of bin and edges. order can be followed by features list.
        self._prob_dist_categorical = []

        self._features = []

    def fit(self, training_feature_values, training_feature_names, num_bins, pred_bins):
        """
        Function is responsible for fitting training data and fill up _prob_dist_categorical containing tuples of bin and edges.
        :rtype: self
        """
        if isinstance(training_feature_values, np.ndarray):

            prob_dist = self._cal_hist_params(training_feature_values, num_bins=num_bins, pred_bins=pred_bins)
            self._prob_dist_categorical = prob_dist
            self._features = training_feature_names

        else:
            raise Exception("categorical histograms are generated on numpy array only!")

        return self

    def get_feature_histogram_rep(self):
        """
        Function is responsible for creating formatted representation of categorical histogram. It will be used in forwarding stat through MLOps.
        :rtype: list containing CategoricalHistogramDataObject
        """
        feature_histogram_rep = []

        for index in range(len(self._features)):
            edges = self._prob_dist_categorical[index][0]
            # Since, edges can be of type string, bins array in python tuple can be stored as string.
            # To make it strong, converting it to float on fly.
            bins_str = self._prob_dist_categorical[index][1]
            # Converting to float!
            bins = [float(i) for i in bins_str]

            normalized_bins = bins / np.sum(bins)
            edges_rep = []
            for each_edge_index in range(0, len(edges)):
                edges_rep.append(str(edges[each_edge_index]))

            categorical_histogram_data_object = CategoricalHistogramDataObject(feature_name=self._features[index],
                                                                               edges=edges_rep,
                                                                               bins=normalized_bins)
            feature_histogram_rep.append(categorical_histogram_data_object)
        return feature_histogram_rep

    @staticmethod
    def _cal_hist_params(sample, num_bins, pred_bins=None):
        """
        Calculate the probability of each category in each column, assuming multi-nomial distribution.

        :param sample: A dataset that is a 2D numpy array
        :param num_bins: Number of bins to create. Although it is not used right now. But to make it scalable, passing now.
        :param pred_bins: pre-defined bins. Although it is not used right now. But to make it scalable, passing now.
        :rtype: prob_dist: A list containing the probability distribution of categories in each column of the sample.
                                   Order of arrays in the list is same as the order of columns in sample data
        """

        # convert whole nd array to float!
        sample = sample.astype(str)

        prob_dist = []
        for a in range(0, sample.shape[1]):
            # Determine the frequency of unique values
            unique, counts = np.unique(sample[:, a], return_counts=True)

            prob_dist.append(np.asarray((unique, counts * 1.0)))
        return prob_dist
