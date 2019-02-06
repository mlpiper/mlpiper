import abc
from pyspark.rdd import RDD

from parallelm.components.spark_session_component import SparkSessionComponent
from parallelm.common.mlcomp_exception import MLCompException


class SparkContextComponent(SparkSessionComponent):
    def __init__(self, ml_engine):
        super(SparkContextComponent, self).__init__(ml_engine)

    def materialize(self, parent_data_objs):
        rdds = self._materialize(self._ml_engine.context, parent_data_objs, self._ml_engine.user_data)
        return rdds

    def _validate_output(self, rdds):
        if rdds:
            if type(rdds) is not list:
                raise MLCompException("Invalid non-list output! Expecting for a list of RDDs!")

            for rdd in rdds:
                if not issubclass(rdd.__class__, RDD):
                    raise MLCompException("Invalid returned list of rdd types! Expecting for 'pyspark.rdd.RDD'! "
                                          "name: {}, type: {}".format(self.name(), type(df)))

    def _materialize(self, spark, parent_data_objs, user_data):
        # A dummy override, because it'll never be called
        raise RuntimeError("The SparkContextComponent._materialize is not supposed to be called!")

    @abc.abstractmethod
    def _materialize(self, sc, parent_data_objs, user_data):
        pass
