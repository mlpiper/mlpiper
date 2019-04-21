import gzip
import os
import pickle
import tempfile

from parallelm.common import constants
from parallelm.common.mlcomp_exception import MLCompException
from parallelm.components import ConnectableComponent


class SageMakerDatasetLoader(ConnectableComponent):

    def __init__(self, engine):
        super(self.__class__, self).__init__(engine)

    def _materialize(self, parent_data_objs, user_data):
        self._setup_env()

        # bucket_name = 'sagemaker-sample-data-{}'.format(region)
        bucket_name = self._params.get('bucket_name')
        if not bucket_name:
            raise MLCompException("Bucket name was not provided!")

        # 'algorithms/kmeans/mnist/mnist.pkl.gz
        dataset_filepath = self._params.get('dataset_filepath')
        if not dataset_filepath:
            raise MLCompException("Missing dataset file path!")

        # boto3 import should be placed after environment setup of AWS credentials and configuration
        import boto3

        tmp_dataset_filepath = tempfile.TemporaryFile()
        self._logger.info("Temporary dataset file path: {}".format(tmp_dataset_filepath))
        try:
            boto3.Session() \
                .resource('s3', region_name=boto3.Session().region_name) \
                .Bucket(bucket_name) \
                .download_file(dataset_filepath, tmp_dataset_filepath)

            with gzip.open(tmp_dataset_filepath, 'rb') as f:
                train_set, valid_set, test_set = pickle.load(f, encoding='latin1')

            return [train_set, valid_set, test_set]
        finally:
            tmp_dataset_filepath.close()
            if os.path.isfile(tmp_dataset_filepath):
                self._logger.info("Cleaning up temporary dataset file path: {}".format(tmp_dataset_filepath))
                os.unlink(tmp_dataset_filepath)

    def _setup_env(self):
        # This function should be placed at the SageMaker engine initialization and the parameters
        # should be part of the execution environment
        access_key_id = self._params.get('aws_access_key_id')
        if not access_key_id:
            raise MLCompException("Missing 'aws_access_key_id' parameter!")

        secret_access_key = self._params.get('aws_secret_access_key')
        if not secret_access_key:
            raise MLCompException("Missing 'aws_secret_access_key' parameter!")

        region = self._params.get('region', 'us-west-2')

        logging_level_name = self._params.get('aws_logging_level', 'info').lower()
        logging_level = constants.LOG_LEVELS.get(logging_level_name, 'info')

        os.environ['AWS_ACCESS_KEY_ID'] = 'AKIAJHWNRUZE3LJMNFIA'
        os.environ['AWS_SECRET_ACCESS_KEY'] = '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'
        os.environ['AWS_DEFAULT_REGION'] = 'us-west-2'

        # boto3 import should be placed after environment setup of AWS credentials and configuration
        import boto3
        boto3.set_stream_logger('boto3.resources', logging_level)

