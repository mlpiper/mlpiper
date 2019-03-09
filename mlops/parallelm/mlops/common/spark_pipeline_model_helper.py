import os
import uuid
import distutils.dir_util
import shutil

from pyspark.ml import PipelineModel
from parallelm.mlops.base_obj import BaseObj


class SparkPipelineModelHelper(BaseObj):
    """
    Class is responsible for providing abstract apis to load and save Spark ML model.
    TODO: We are using HDFS/NFS as interim fs to save file before it goes to backend.
    """

    def __init__(self):
        super(SparkPipelineModelHelper, self).__init__(__name__)
        self._spark_context = None

        self._hadoop = None
        self._model = None
        self._hadoop_fs = None

        # the ones below needs to be provided by user
        self._local_path = None
        self._current_tmp_dir = None
        self._shared_path_prefix = None

        # type of shared space provided
        self._type = None

        # this are preset variables.
        self._model_dir_name_prefix = "model_dir_"
        self._model_dir_name = "MODEL"

    def set_local_path(self, local_path):
        """
        set the local path - a local path is MCenter system provided. This path is local to the
        spark driver (and MCenter Agent).
        :param local_path: the path provided by MCenter (as the model path)
        :return: None
        """
        if local_path.startswith("file://"):
            self._local_path = local_path.partition("file://")[2]
        else:
            self._local_path = local_path
        return self

    def set_shared_path_prefix(self, shared_path_prefix):
        """
        set the shared path prefix - this is a user provided scratch space, which is accessible
        from any node on the spark cluster. This can either be a hdfs path or a nfs path.
        :param shared_path_prefix: a user provided scratch space path
        :return: None
        """
        if shared_path_prefix is None:
            self._shared_path_prefix = None
            return self
        if shared_path_prefix.startswith("hdfs://") or shared_path_prefix.startswith("file://"):
            parts = shared_path_prefix.partition("://")
            self._type = parts[0]
            self._shared_path_prefix = parts[2]
        else:
            raise Exception("Invalid prefix path type {}".format(shared_path_prefix))
        return self

    def set_shared_context(self, hadoop=None, spark_context=None):
        """
        Provide the hadoop or spark context - this will be used when files are copied into shared
        and loaded into spark.
        :param hadoop: hadoop handle
        :param spark_context: spark context
        :return: None
        """
        self._spark_context = spark_context
        if hadoop is not None:
            self._hadoop = hadoop
        elif spark_context is not None:
            self._hadoop = self._spark_context._gateway.jvm.org.apache.hadoop

        if self._hadoop is not None:
            config = self._hadoop.conf.Configuration()
            self._hadoop_fs = self._hadoop.fs.FileSystem.get(config)
        return self

    def copy_to_shared_path(self, shared_path):
        """
        Copy a local path (MCenter provided) to a shared path location
        :param shared_path: a user provided path (either hdfs or nfs)
        :return: None
        """
        if self._local_path is None:
            raise Exception("Local path needs to be specified to complete `copy_to_shared_path`")

        if shared_path is None:
            raise Exception("Shared path needs to be specified to complete `copy_to_shared_path`")

        if self._type == "hdfs" and self._hadoop is None:
            raise Exception("Hadoop handle not provided or configured, though shared path was hdfs")

        if self._type == "hdfs":
            # file was trimmed when set_local_path was called
            local_path = "file://" + self._local_path

            # shared path may be provided by user or created by this library
            # so better to check for both cases (hdfs prefix is present or not)
            if shared_path.startswith("hdfs://") is False:
                shared_path = "hdfs://" + shared_path

            dst_path = self._hadoop.fs.Path(shared_path)
            src_path = self._hadoop.fs.Path(local_path)

            self._logger.info("Accessing HDFS - moving from {} to {}".format(src_path, dst_path))

            # copying local model to hdfs
            self._hadoop_fs.moveFromLocalFile(src_path, dst_path)
        else:
            # just copy
            self._logger.info("File copy - from {} to {}".format(self._local_path, shared_path))
            distutils.dir_util.copy_tree(self._local_path, shared_path)

    def copy_from_shared_path(self, shared_path):
        """
        Copy from a shared path to the local path
        :param shared_path: a user provided shared path containing the model
        :return: None
        """
        if shared_path is None:
            raise Exception("Shared path needs to be specified to complete `copy_from_shared_path`")
        if self._local_path is None:
            raise Exception("Local path needs to be specified to complete `copy_from_shared_path`")

        if self._type == "hdfs":
            local_path = "file://" + self._local_path

            if shared_path.startswith("hdfs://") is False:
                shared_path = "hdfs://" + shared_path

            src_path = self._hadoop.fs.Path(shared_path)
            dst_path = self._hadoop.fs.Path(local_path)

            self._logger.info("Accessing HDFS - moving from {} to {}".format(src_path, dst_path))

            # copying local model to hdfs
            self._hadoop_fs.moveToLocalFile(src_path, dst_path)
        else:
            # just copy
            self._logger.info("File copy from {} to {}".format(shared_path, self._local_path))
            distutils.dir_util.copy_tree(shared_path, self._local_path)

    def create_unique_shared_path(self):
        """
        Use the user provided scratch space and create a new unique path to save models
        TODO: kindly check before returning the path
        :return: unique path, randomly generated
        """
        if self._shared_path_prefix is None:
            raise Exception("Shared path prefix must be set")

        self._current_tmp_dir = os.path.join(self._shared_path_prefix,
                                             self._model_dir_name_prefix +
                                             str(uuid.uuid4()))
        return self._current_tmp_dir

    def delete_shared_tmp_file(self):
        """
        Delete the unique path that was created earlier
        :return: None
        """
        if self._current_tmp_dir is None:
            raise Exception("No temp path created - cannot delete it")

        if self._hadoop is not None:
            path = self._hadoop.fs.Path(self._current_tmp_dir)
            self._hadoop_fs.delete(path)
        else:
            shutil.rmtree(self._current_tmp_dir)
        self._current_tmp_dir = None

    def load_sparkml_model(self):
        """
        A user facing wrapper that can load a sparkml model from the MCenter-provided
        local path (via the shared space)
        :return: sparkml model
        """

        # don't do anything if the path was preset with necessary information
        if self._local_path.startswith("hdfs://") or self._shared_path_prefix is None:
            self._logger.info("Direct load from {}".format(self._local_path))
            return PipelineModel.load(self._local_path)

        # we may have to walk down the tree to find the right spark-ml folder
        for file in os.listdir(self._local_path):
            directory_inside_model_path = os.path.join(self._local_path, file)
            for each_file in os.listdir(directory_inside_model_path):
                if "metadata" in each_file:
                    self._local_path = directory_inside_model_path
                    self._logger.info("Switching to {} for model".format(self._local_path))
                    break
        try:
            # create a shared path
            tmp_path = self.create_unique_shared_path()
            # copy model from local path to shared path
            self.copy_to_shared_path(tmp_path)
            # load the model
            model = PipelineModel.load(tmp_path)
            # delete local file
            self.delete_shared_tmp_file()
            return model
        except Exception as e:
            self._logger.error("Not able to load model because of error: {}".format(e))
            return None

    def save_sparkml_model(self, model):
        """
        A user facing wrapper that helps save a spark-ml model to the MCenter provided local
        path (via the shared space)
        :param model: spark-ml model
        :return: None
        """

        # save directly, dont attempt transfer via shared space
        if self._local_path.startswith("hdfs://") or self._shared_path_prefix is None:
            self._logger.info("Direct write to {}".format(self._local_path))
            model.write().overwrite().save(self._local_path)
            return

        try:
            # create a shared path
            tmp_path = self.create_unique_shared_path()
            # save model to shared path
            model.write().overwrite().save(tmp_path)
            # copy model from shared path to local path
            self.copy_from_shared_path(tmp_path)
            # delete shared path
            self.delete_shared_tmp_file()
        except Exception as e:
            self._logger.error("Not able to save model because of error: {}".format(e))
            return
