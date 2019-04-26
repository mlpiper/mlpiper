import io
import time
from time import gmtime, strftime

from sagemaker.session import Session

import boto3
from sagemaker.amazon.amazon_estimator import get_image_uri
from sagemaker.amazon.common import write_numpy_to_dense_tensor

from parallelm.common.mlcomp_exception import MLCompException
from parallelm.components import ConnectableComponent

from common.aws_helper import AwsHelper
from common.report import Report


class SageMakerKMeansTrainerTest(ConnectableComponent):
    MONITOR_INTERVAL_SEC = 10.0

    def __init__(self, engine):
        super(SageMakerKMeansTrainerTest, self).__init__(engine)
        self._bucket_name = None
        self._train_set = None
        self._data_location = None
        self._data_s3_url = None
        self._output_location = None
        self._model_artifact_s3_url = None
        self._output_model_filepath = None
        self._kmeans = None
        self._job_name = None
        self._sagemaker_client = boto3.client('sagemaker')
        self._aws_helper = AwsHelper(self._logger)

    def _materialize(self, parent_data_objs, user_data):

        if not parent_data_objs or len(parent_data_objs) != 3:
            raise MLCompException("Expecting 3 parent inputs! got: {}, parent_data: {}"
                                  .format(len(parent_data_objs), parent_data_objs))

        self._init_params(parent_data_objs)
        self._convert_and_upload()
        self._do_training()
        self._download_model()

    def _init_params(self, parent_data_objs):
        self._output_model_filepath = self._params['output_model_filepath']

        self._train_set, valid_set, test_set = parent_data_objs
        self._print_statistics_info(self._train_set, valid_set, test_set)

        self._bucket_name = self._params.get('bucket_name')
        if not self._bucket_name:
            self._bucket_name = Session().default_bucket()

        self._data_location = self._params.get('data_location')
        if not self._data_location:
            self._data_location = 'training/kmeans/data'

        self._output_location = self._params.get('output_location')
        if not self._output_location:
            self._output_location = 's3://{}/training/kmeans/output'.format(self._bucket_name)
        else:
            self._output_location = 's3://{}/{}'.format(self._bucket_name, self._output_location)

    def _print_statistics_info(self, train_set, valid_set, test_set):
        self._logger.info("Number of samples in training set: {}".format(len(train_set[0])))
        self._logger.info("Number of samples in valid set: {}".format(len(valid_set[0])))
        self._logger.info("Number of samples in test set: {}".format(len(test_set[0])))
        self._logger.info("First image caption in training set: '{}'".format(train_set[1][0]))

    def _convert_and_upload(self):
        self._logger.info("Converting the data into the format required by the SageMaker KMeans algorithm ...")
        buf = io.BytesIO()
        write_numpy_to_dense_tensor(buf, self._train_set[0], self._train_set[1])
        buf.seek(0)

        self._logger.info("Uploading the converted data to S3, bucket: {}, location: {} ..."
                          .format(self._bucket_name, self._data_location))
        self._data_s3_url = self._aws_helper.upload_file_obj(buf, self._bucket_name, self._data_location)

    def _do_training(self):
        self._logger.info('Training data is located in: {}'.format(self._data_s3_url))
        self._logger.info('Artifacts will be located in: {}'.format(self._output_location))

        self._job_name = 'kmeans-batch-training-' + strftime("%Y-%m-%d-%H-%M-%S", gmtime())
        image = get_image_uri(boto3.Session().region_name, 'kmeans')

        create_training_params = \
            {
                "AlgorithmSpecification": {
                    "TrainingImage": image,
                    "TrainingInputMode": "File"
                },
                "RoleArn": self._ml_engine.iam_role,
                "OutputDataConfig": {
                    "S3OutputPath": self._output_location
                },
                "ResourceConfig": {
                    "InstanceCount": 2,
                    "InstanceType": "ml.c4.xlarge",
                    "VolumeSizeInGB": 50
                },
                "TrainingJobName": self._job_name,
                "HyperParameters": {
                    "k": "10",
                    "feature_dim": "784",
                    "mini_batch_size": "500",
                    "force_dense": "True"
                },
                "StoppingCondition": {
                    "MaxRuntimeInSeconds": 60 * 60
                },
                "InputDataConfig": [
                    {
                        "ChannelName": "train",
                        "DataSource": {
                            "S3DataSource": {
                                "S3DataType": "S3Prefix",
                                "S3Uri": self._data_s3_url,
                                "S3DataDistributionType": "FullyReplicated"
                            }
                        },
                        "CompressionType": "None",
                        "RecordWrapperType": "None"
                    }
                ]
            }

        self._logger.info("Creating training job ... {}".format(self._job_name))
        self._sagemaker_client.create_training_job(**create_training_params)

        self._monitor_job()

    def _monitor_job(self):
        self._logger.info("Monitoring training job ... {}".format(self._job_name))
        index = 1
        while True:
            response = self._sagemaker_client.describe_training_job(TrainingJobName=self._job_name)
            status = response['TrainingJobStatus']
            Report.job_status(self._job_name, status)
            if status == 'Completed':
                self._model_artifact_s3_url = response['ModelArtifacts']['S3ModelArtifacts']
                self._logger.info("Training job ended! status: {}".format(status))
                break
            if status == 'Failed':
                msg = 'Training job failed! message: {}'.format(response['FailureReason'])
                self._logger.error(msg)
                raise MLCompException(msg)
            self._logger.info("Training job is still running, status: {} ... {} sec"
                              .format(status, index * SageMakerKMeansTrainerTest.MONITOR_INTERVAL_SEC))
            index += 1
            time.sleep(SageMakerKMeansTrainerTest.MONITOR_INTERVAL_SEC)

    def _download_model(self):
        if self._output_model_filepath and self._model_artifact_s3_url:
            self._logger.info("Downloading model, {} ==> {}"
                              .format(self._model_artifact_s3_url, self._output_model_filepath))
            AwsHelper(self._logger).download_file(self._model_artifact_s3_url, self._output_model_filepath)
