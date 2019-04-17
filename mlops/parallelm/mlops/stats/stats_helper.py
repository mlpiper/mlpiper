import numpy as np
import six

from parallelm.mlops.base_obj import BaseObj
from parallelm.mlops.constants import Constants
from parallelm.mlops.metrics_constants import ClassificationMetrics
from parallelm.mlops.ml_metrics_stat.accuracy_score import AccuracyScore
from parallelm.mlops.ml_metrics_stat.auc import AUC
from parallelm.mlops.ml_metrics_stat.average_precision_score import AveragePrecisionScore
from parallelm.mlops.ml_metrics_stat.confusion_matrix import ConfusionMatrix
from parallelm.mlops.mlops_exception import MLOpsException, MLOpsStatisticsException
from parallelm.mlops.stats.bar_graph import BarGraph
from parallelm.mlops.stats.kpi_value import KpiValue
from parallelm.mlops.stats.mlops_stat_getter import MLOpsStatGetter
from parallelm.mlops.stats_category import StatCategory


class StatsHelper(BaseObj):
    def __init__(self, output_channel):
        super(StatsHelper, self).__init__(__name__)
        self._output_channel = output_channel
        self._curr_model_stat = None
        self._api_test_mode = False

    def _validate_supported_conf_ts_data_type(self, data):
        if isinstance(data, (six.integer_types, float, six.string_types)):
            pass
        elif isinstance(data, list):
            first_item_type = type(data[0])
            for item in data:
                if type(item) != first_item_type:
                    raise MLOpsException("Error: detected at least 2 types of items in list {} and {}"
                                         .format(first_item_type, type(item)))
            if not isinstance(data[0], (float, six.integer_types)):
                raise MLOpsException("Only arrays of int or float are supported")
        elif isinstance(data, dict):
            pass
        elif isinstance(data, np.ndarray):
            pass
        else:
            raise MLOpsException("Type : {} is not yet supported by {}".
                                 format(type(data).__name__, Constants.OFFICIAL_NAME))

    def _set_classification_stat(self, name, data, model_id, timestamp, **kwargs):
        mlops_stat_object = None
        category = StatCategory.GENERAL

        self._logger.debug("{} predefined stat called: name: {} data_type: {}".
                           format(Constants.OFFICIAL_NAME, name, type(data)))

        if name == ClassificationMetrics.CONFUSION_MATRIX:
            mlops_stat_object = \
                ConfusionMatrix.get_mlops_cm_table_object(cm_nd_array=data, **kwargs)

        elif name == ClassificationMetrics.ACCURACY_SCORE:
            mlops_stat_object = \
                AccuracyScore.get_mlops_accuracy_stat_object(accuracy_score=data)
            category = StatCategory.TIME_SERIES

        elif name == ClassificationMetrics.AUC:
            mlops_stat_object = \
                AUC.get_mlops_auc_stat_object(auc=data)
            category = StatCategory.TIME_SERIES

        elif name == ClassificationMetrics.AVERAGE_PRECISION_SCORE:
            mlops_stat_object = \
                AveragePrecisionScore.get_mlops_aps_stat_object(aps=data)
            category = StatCategory.TIME_SERIES

        if mlops_stat_object is not None:
            self.set_stat(name=name,
                          data=mlops_stat_object,
                          model_id=model_id,
                          # type of stat will be General
                          category=category,
                          timestamp=timestamp,
                          **kwargs)
        else:
            error = "{} predefined stat cannot be output as error happened in creating mlops stat object from {}" \
                .format(name, data)

            raise MLOpsStatisticsException(error)

    def set_stat(self, name, data, model_id, category, timestamp, **kwargs):
        # If name supports the stat_object API, return the object.
        if isinstance(name, MLOpsStatGetter):
            self._output_channel.stat_object(name.get_mlops_stat(model_id))
            return self

        # If data supports the stat_object API, return the object.
        elif isinstance(data, MLOpsStatGetter):
            self._output_channel.stat_object(data.get_mlops_stat(model_id))
            return self

        if name in ClassificationMetrics:
            self._set_classification_stat(name=name,
                                          data=data,
                                          model_id=model_id,
                                          timestamp=timestamp, **kwargs)

            return self

        if category in (StatCategory.CONFIG, StatCategory.TIME_SERIES):
            self._logger.debug("{} stat called: name: {} data_type: {} class: {}".
                               format(Constants.OFFICIAL_NAME, name, type(data), category))

            self._validate_supported_conf_ts_data_type(data)
            self._output_channel.stat(name, data, model_id, category, **kwargs)
        else:
            raise MLOpsException("stat_class: {} not supported in set_stat call".format(category))

    def set_data_distribution_stat(self, data, model_id, model, timestamp):
        self._output_channel.stat("input", data, model_id, StatCategory.INPUT, model, self._curr_model_stat)

    def set_kpi(self, name, data, model_id, timestamp, units):

        if not isinstance(name, six.string_types):
            raise MLOpsException("name argument must be a string")
        if not isinstance(data, (six.integer_types, float)):
            raise MLOpsException("KPI data must be a number")

        kpi_value = KpiValue(name, data, timestamp, units)

        if self._api_test_mode:
            self._logger.info("API testing mode - returning without performing call")
            return

        self._output_channel.stat_object(kpi_value.get_mlops_stat(model_id))

    def feature_importance(self,
                           model_obj,
                           feature_importance_vector=None,
                           feature_names=None,
                           model=None, df=None,
                           num_significant_features=100):
        """
         present feature importance, either according to the provided vector or generated from
         the provided model if available.
         Feature importance bar graph is attached to the current model and can be fetched later for
          this model.
         this function implements:
         1) use feature_importance_vector if exists
         2) feature_names from the model if available

         3) get feature names vector if exists
         4) extract feature name from pipeline model or dataframe if exists -
          (code different to pyspark and sklearn)

         5) sort the vector.
         6) take first k elements
         7) create a bar graph for feature importance

         :param model_obj: model  object
         :param feature_importance_vector: feature importance vector optional
         :param feature_names: feature names vector optional
         :param model: optional pipeline model for pyspark, sklearn model for python
         :param df: optional dataframe for analysis
         :param num_significant_features: Number of significant features
         :raises: MLOpsException
         """

        self._validate_feature_importance_inputs(feature_importance_vector, feature_names, model, df)

        important_named_features = self._output_channel.feature_importance(feature_importance_vector, feature_names,
                                                                           model, df)

        if important_named_features:
            # Sort the feature importance vector
            important_named_features_sorted = sorted(important_named_features,
                                                     key=lambda x: x[1], reverse=True)
            self._logger.info("Full important_named_features_sorted = {}"
                              .format(important_named_features_sorted))

            # output k significant features
            if int(num_significant_features) < len(important_named_features_sorted):
                important_named_features_sorted = important_named_features_sorted[0:int(num_significant_features)]

            # Plot results in a bar graph
            self._logger.info("Important_named_features_sorted = {}"
                              .format(important_named_features_sorted))
            col_names = [v[0] for i, v in enumerate(important_named_features_sorted)]
            col_value = [v[1] for i, v in enumerate(important_named_features_sorted)]
            bar = BarGraph().name("Feature Importance").cols(col_names).data(col_value)
            model_obj.set_stat(bar)

    def _validate_feature_importance_inputs(self, feature_importance_vector=None, feature_names=None, model=None,
                                            df=None):
        """
        verify common parameters. specific parameters are verified in each output channel
        :param feature_importance_vector: feature importance vector optional
        :param feature_names: feature names vector optional
        :param model: optional pipeline model for pyspark, sklearn model for python
        :param df: optional dataframe for analysis
        :raises: MLOpsException
        """

        # check that either model is provided or feature importance vector
        if not feature_importance_vector and not model:
            raise MLOpsException("must provide either feature importance vector or a supporting model")
        # check that either df is provided or feature names vector
        if df is None and not feature_names:
            raise MLOpsException("must provide either feature names vector or a dataframe that can provide the names")

        if feature_importance_vector:
            if not isinstance(feature_importance_vector, list):
                raise MLOpsException("features importance vector must be a list")
            for feature_importance_element in feature_importance_vector:
                if not isinstance(feature_importance_element, (six.integer_types, float)):
                    raise MLOpsException(
                        "features importance elements must be a number. got: {} ".format(feature_importance_element))
        if feature_names:
            if not isinstance(feature_names, list):
                raise MLOpsException("features names vector must be a list")
            for feature_names_element in feature_names:
                if not isinstance(feature_names_element, six.string_types):
                    raise MLOpsException(
                        "features name elements must be a string. got: {} ".format(feature_names_element))
