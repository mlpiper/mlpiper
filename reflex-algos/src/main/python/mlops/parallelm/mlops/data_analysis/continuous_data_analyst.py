import pandas as pd


class ContinuousDataAnalyst(object):
    def __init__(self):
        # type: () -> object
        pass

    @staticmethod
    def analyze(set_of_continuous_feature_names,
                set_of_continuous_feature_values):
        """
        Right now, method works on python data structures.
        :param set_of_continuous_feature_names: List of feature names in same order as value provided.
        :param set_of_continuous_feature_values: 2D array of values. Each column represents each feature values.
        :return: Analysis result map containing feature name as key and result structure associated in value
        """

        analysis_result = {}

        # generating DA if features_values exists.
        if len(set_of_continuous_feature_values) > 0:

            pandas_continuous_df = pd.DataFrame(data=set_of_continuous_feature_values.astype(float),
                                                columns=set_of_continuous_feature_names)

            size = len(pandas_continuous_df)

            if size > 0:

                for each_features in pandas_continuous_df.keys():
                    analysis_result[each_features] = ContinuousDataAnalysisResult(each_features)
                    analysis_result[each_features].count = size

                nas_sum = pandas_continuous_df.isnull().sum(axis=0)
                for (each_features, features_nas_sum) in nas_sum.iteritems():
                    analysis_result[each_features].NAs = str(features_nas_sum * 100.0 / size) + "%"

                mins = pandas_continuous_df.min()
                for (each_features, features_mins) in mins.iteritems():
                    analysis_result[each_features].min = features_mins

                maxs = pandas_continuous_df.max()
                for (each_features, features_maxs) in maxs.iteritems():
                    analysis_result[each_features].max = features_maxs

                means = pandas_continuous_df.mean()
                for (each_features, features_means) in means.iteritems():
                    analysis_result[each_features].mean = features_means

                medians = pandas_continuous_df.median()
                for (each_features, features_medians) in medians.iteritems():
                    analysis_result[each_features].median = features_medians

                stds = pandas_continuous_df.std()
                for (each_features, features_stds) in stds.iteritems():
                    analysis_result[each_features].std = features_stds

                zeros = (pandas_continuous_df == 0.0).sum(axis=0)
                for (each_features, features_zeros) in zeros.iteritems():
                    analysis_result[each_features].zeros = features_zeros

        return analysis_result


class ContinuousDataAnalysisResult(object):

    def __init__(self,
                 feature_name,
                 count=None,
                 NAs=None,
                 min=None,
                 max=None,
                 mean=None,
                 median=None,
                 std=None,
                 zeros=None):
        self._feature_name = feature_name
        self._count = count
        self._NAs = NAs
        self._min = min
        self._max = max
        self._mean = mean
        self._median = median
        self._std = std
        self._zeros = zeros

    def __str__(self):
        return str("feature_name = " + str(self._feature_name) + "\n"
                   + "count = " + str(self._count) + "\n"
                   + "NAs = " + str(self._NAs) + "\n"
                   + "min = " + str(self._min) + "\n"
                   + "max = " + str(self._max) + "\n"
                   + "mean = " + str(self._mean) + "\n"
                   + "median = " + str(self._median) + "\n"
                   + "std = " + str(self._std) + "\n"
                   + "zeros = " + str(self._zeros) + "\n")

    @property
    def feature_name(self):
        return self._feature_name

    @feature_name.setter
    def feature_name(self, value):
        self._feature_name = value

    @property
    def count(self):
        return self._count

    @count.setter
    def count(self, value):
        self._count = value

    @property
    def NAs(self):
        return self._NAs

    @NAs.setter
    def NAs(self, value):
        self._NAs = value

    @property
    def min(self):
        return self._min

    @min.setter
    def min(self, value):
        self._min = value

    @property
    def max(self):
        return self._max

    @max.setter
    def max(self, value):
        self._max = value

    @property
    def mean(self):
        return self._mean

    @mean.setter
    def mean(self, value):
        self._mean = value

    @property
    def median(self):
        return self._median

    @median.setter
    def median(self, value):
        self._median = value

    @property
    def std(self):
        return self._std

    @std.setter
    def std(self, value):
        self._std = value

    @property
    def zeros(self):
        return self._zeros

    @zeros.setter
    def zeros(self, value):
        self._zeros = value

    def __eq__(self, other):
        return (self._feature_name == other._feature_name and
                self._count == other._count and
                # comparing first 3 letters only
                self._NAs[:3] == other._NAs[:3] and
                self._min == other._min and
                self._max == other._max and
                (self._mean - other._mean) < 0.01 and
                self._median == other._median and
                (self._std - other._std) < 0.01 and
                self._zeros == other._zeros)
