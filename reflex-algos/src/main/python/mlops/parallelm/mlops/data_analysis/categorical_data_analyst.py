import pandas as pd


class CategoricalDataAnalyst(object):
    """
    Class provides categorical data analysis capability.
    In future, class will be used by cross platform but right now implementation is limited to python data structures only!
    Class outputs #Count, #NAs, #UniqueCategories, Top Occuring Category, #Occurence Of Top Category, Average Length Of Category
    """

    def __init__(self):
        # type: () -> object
        pass

    @staticmethod
    def analyze(set_of_categorical_feature_names,
                set_of_categorical_feature_values):
        """
        Right now, method works on python data structures.
        :param set_of_categorical_feature_names: List of feature names in same order as value provided.
        :param set_of_categorical_feature_values: 2D array of values. Each column represents each feature values.
        :return: Analysis result map containing feature name as key and result structure associated in value
        """
        analysis_result = {}

        # generating DA if features_values exists.
        if len(set_of_categorical_feature_values) > 0:

            pandas_categorical_df = pd.DataFrame(data=set_of_categorical_feature_values,
                                                 columns=set_of_categorical_feature_names)

            analysis_result = {}
            size = len(pandas_categorical_df)
            if size > 0:

                for each_features in pandas_categorical_df.keys():
                    analysis_result[each_features] = CategoricalDataAnalysisResult(each_features)
                    analysis_result[each_features].count = size

                nas_sum = pandas_categorical_df.isnull().sum(axis=0)
                for (each_features, features_nas_sum) in nas_sum.iteritems():
                    analysis_result[each_features].NAs = str(features_nas_sum * 100.0 / size) + "%"

                nuniques = pandas_categorical_df.nunique()
                for (each_features, features_nuniques) in nuniques.iteritems():
                    analysis_result[each_features].unique = features_nuniques

                for (each_features, each_feature_value) in pandas_categorical_df.iteritems():
                    value_count = each_feature_value.value_counts()
                    if len(value_count) > 0:
                        analysis_result[each_features].top = str(value_count.index.values[0])
                        analysis_result[each_features].freq_top = value_count.values[0]
                    else:
                        analysis_result[each_features].top = "N/A"
                        analysis_result[each_features].freq_top = 0.0

                for (each_features, each_feature_value) in pandas_categorical_df.iteritems():
                    no_nas = each_feature_value.dropna()
                    if len(no_nas) > 0:
                        analysis_result[each_features].avg_str_len = no_nas.apply(str).str.len().mean()
                    else:
                        analysis_result[each_features].avg_str_len = 0

        return analysis_result


class CategoricalDataAnalysisResult(object):

    def __init__(self,
                 feature_name,
                 count=None,
                 NAs=None,
                 unique=None,
                 top=None,
                 freq_top=None,
                 avg_str_len=None):
        self._feature_name = feature_name
        self._count = count
        self._NAs = NAs
        self._unique = unique
        self._top = top
        self._freq_top = freq_top
        self._avg_str_len = avg_str_len

    def __str__(self):
        return str("feature_name = " + str(self._feature_name) + "\n"
                   + "count = " + str(self._count) + "\n"
                   + "NAs = " + str(self._NAs) + "\n"
                   + "unique = " + str(self._unique) + "\n"
                   + "top = " + str(self._top) + "\n"
                   + "freq_top = " + str(self._freq_top) + "\n"
                   + "avg_str_len = " + str(self._avg_str_len))

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
    def unique(self):
        return self._unique

    @unique.setter
    def unique(self, value):
        self._unique = value

    @property
    def top(self):
        return self._top

    @top.setter
    def top(self, value):
        self._top = value

    @property
    def freq_top(self):
        return self._freq_top

    @freq_top.setter
    def freq_top(self, value):
        self._freq_top = value

    @property
    def avg_str_len(self):
        return self._avg_str_len

    @avg_str_len.setter
    def avg_str_len(self, value):
        self._avg_str_len = value

    def __eq__(self, other):
        return (self._feature_name == other._feature_name and
                self._count == other._count and
                # comparing first 3 letters only
                self._NAs[:3] == other._NAs[:3] and
                self._unique == other._unique and
                self._top == other._top and
                self._freq_top == other._freq_top and
                (self._avg_str_len - other._avg_str_len) < 0.01)
