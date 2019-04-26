import boto3
import os
from time import gmtime, strftime

from sagemaker.amazon.amazon_estimator import get_image_uri

try:  # Python3
    from urllib.parse import urlparse, urlencode
except ImportError:  # Python2
    from urlparse import urlparse
    from urllib import urlencode


class AwsHelper(object):
    def __init__(self, logger):
        self._logger = logger

    def upload_file(self, local_filepath, bucket_name, aws_s3_filepath, skip_upload=False):
        if aws_s3_filepath:
            if aws_s3_filepath.endswith('/'):
                aws_s3_filepath += os.path.basename(local_filepath)
        else:
            aws_s3_filepath = os.path.basename(local_filepath)

        s3_url = 's3://{}/{}'.format(bucket_name, aws_s3_filepath)
        self._logger.info("Uploading file to S3: {} ==> {}".format(local_filepath, s3_url))

        if not skip_upload:
            boto3.client('s3').upload_file(local_filepath, bucket_name, aws_s3_filepath)

        self._logger.info("File uploaded successfully!")

        return s3_url

    def upload_file_obj(self, file_obj, bucket_name, aws_s3_filepath, skip_upload=False):
        s3_url = 's3://{}/{}'.format(bucket_name, aws_s3_filepath)
        self._logger.info("Uploading file obj to S3 ... {}".format(s3_url))

        if not skip_upload:
            boto3.resource('s3').Bucket(bucket_name).Object(aws_s3_filepath).upload_fileobj(file_obj)

        self._logger.info("File obj uploaded successfully!")

        return s3_url

    def download_file(self, aws_s3_url, local_filepath):
        self._logger.info("Downloading file from S3: {}, to: {}".format(aws_s3_url, local_filepath))
        bucket_name, model_path = AwsHelper.s3_url_parse(aws_s3_url)

        s3 = boto3.resource('s3')
        s3.Bucket(bucket_name).download_file(model_path, local_filepath)

        self._logger.info("File downloaded successfully!")

    @staticmethod
    def s3_url_parse(aws_s3_url):
        remove_leading_slash = lambda p: p[1:] if p[0] == '/' else p

        parsed_url = urlparse(aws_s3_url)

        if parsed_url.scheme == 's3':
            bucket_name = parsed_url.netloc
            rltv_path = remove_leading_slash(parsed_url.path)
        else:
            path = remove_leading_slash(parsed_url.path)
            path_parts = path.split('/', 1)
            bucket_name = path_parts[0]
            rltv_path = path_parts[1]

        return bucket_name, rltv_path

    def kmeans_image_uri(self):
        return get_image_uri(boto3.Session().region_name, 'kmeans')
