
import boto3
from collections import namedtuple
from datetime import timedelta
import logging
import pprint
import time

from monitor.job_monitor_base import JobMonitorBase
from monitor.report import Report
from parallelm.common.mlcomp_exception import MLCompException


class JobMonitorTransformer(JobMonitorBase):
    INSTANCE_ID_FETCHING_NUM_RETRIES = 5
    MetricMeta = namedtuple('MetricMeta', ['id', 'metric_name', 'stat'])

    def __init__(self, sagemaker_client, job_name, logger):
        super(self.__class__, self).__init__(sagemaker_client, job_name, logger)

        self._cloudwatch_client = boto3.client('cloudwatch')

        self._metrics_defs = [
            JobMonitorTransformer.MetricMeta('cpuavg', 'CPUUtilization', 'Average'),
            JobMonitorTransformer.MetricMeta('cpumin', 'CPUUtilization', 'Minimum'),
            JobMonitorTransformer.MetricMeta('cpumax', 'CPUUtilization', 'Maximum'),
            JobMonitorTransformer.MetricMeta('memavg', 'MemoryUtilization', 'Average'),
            JobMonitorTransformer.MetricMeta('memmin', 'MemoryUtilization', 'Minimum'),
            JobMonitorTransformer.MetricMeta('memmax', 'MemoryUtilization', 'Maximum')
        ]

    def _describe_job(self):
        return self._sagemaker_client.describe_transform_job(TransformJobName=self._job_name)

    def _job_status(self, describe_response):
        return describe_response['TransformJobStatus']

    def _report_online_metrics(self, describe_response):
        pass

    def _report_final_metrics(self, describe_response):
        job_instance_id = self._get_transform_job_instance_id()
        self._logger.info("Job instance id: {}".format(job_instance_id))
        if job_instance_id:
            metrics_data = self._get_transform_job_metrics(job_instance_id, describe_response)
            Report.transform_job_final_metrics(self._job_name, metrics_data)
        else:
            self._logger.warning("Skip transform job metrics reporting!")

    def _get_transform_job_instance_id(self):
        for index in range(JobMonitorTransformer.INSTANCE_ID_FETCHING_NUM_RETRIES):
            response = self._cloudwatch_client.list_metrics(Namespace='/aws/sagemaker/TransformJobs',
                                                            MetricName='CPUUtilization')
            if self._logger.isEnabledFor(logging.DEBUG):
                self._logger.debug(pprint.pformat(response, indent=4))

            for metric in response['Metrics']:
                instance_id = metric['Dimensions'][0]['Value']
                if instance_id.startswith(self._job_name):
                    return instance_id

            if index < JobMonitorTransformer.INSTANCE_ID_FETCHING_NUM_RETRIES - 1:
                time.sleep(2)

        return None


    def _get_transform_job_metrics(self, job_instance_id, describe_response):
        start_time = describe_response['TransformStartTime']
        # Incrementing end time by 2 min since CloudWatch drops seconds before finding the logs.
        # This results in logs being searched in the time range in which the correct log line was not present.
        # Example - Log time - 2018-10-22 08:25:55
        #           Here calculated end time would also be 2018-10-22 08:25:55 (without 1 min addition)
        #           CW will consider end time as 2018-10-22 08:25 and will not be able to search the correct log.
        end_time = describe_response['TransformEndTime'] + timedelta(minutes=2)
        d, r = divmod((end_time - start_time).total_seconds(), 60)
        period = int(d) * 60 + 60  # must be a multiplier of 60
        self._logger.debug("Start time: {}, end time: {}, period: {} sec".format(start_time, end_time, period))

        metric_data_queries = self._metric_data_queries(job_instance_id, period)

        response = self._cloudwatch_client.get_metric_data(
            MetricDataQueries=metric_data_queries,
            StartTime=start_time,
            EndTime=end_time,
            ScanBy='TimestampAscending'
        )
        return response['MetricDataResults']

    def _metric_data_queries(self, job_instance_id, period):
        metric_data_queries = []
        for metric_meta in self._metrics_defs:
            query = {
                'Id': metric_meta.id,
                'MetricStat': {
                    'Metric': {
                        'Namespace': '/aws/sagemaker/TransformJobs',
                        'MetricName': metric_meta.metric_name,
                        'Dimensions': [
                            {
                                'Name': 'Host',
                                'Value': job_instance_id
                            },
                        ]
                    },
                    'Period': period,
                    'Stat': metric_meta.stat
                }
            }
            metric_data_queries.append(query)

        return metric_data_queries
