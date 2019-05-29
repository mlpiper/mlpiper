import boto3
from collections import namedtuple
from datetime import timedelta
import logging
import pprint
import time

from parallelm.extra.sagemaker.monitor.job_monitor_base import JobMonitorBase
from parallelm.extra.sagemaker.monitor.report import Report
from parallelm.extra.sagemaker.monitor.sm_api_constants import SMApiConstants


class JobMonitorTransformer(JobMonitorBase):
    ONLINE_METRICS_FETCHING_NUM_RETRIES = 1
    FINAL_METRICS_FETCHING_NUM_RETRIES = 36
    SLEEP_TIME_BETWEEN_METRICS_FETCHING_RETRIES_SEC = 5.0

    MetricMeta = namedtuple('MetricMeta', ['id', 'metric_name', 'stat'])

    def __init__(self, sagemaker_client, job_name, logger):
        super(self.__class__, self).__init__(sagemaker_client, job_name, logger)

        self._host_metrics_fetched_successfully = False

        self._cloudwatch_client = boto3.client('cloudwatch')

        self._host_metrics_defs = [
            JobMonitorTransformer.MetricMeta('cpuavg_{}', SMApiConstants.Transformer.METRIC_CPU_UTILIZATION,
                                             SMApiConstants.Transformer.STAT_AVG),
            JobMonitorTransformer.MetricMeta('cpumin_{}', SMApiConstants.Transformer.METRIC_CPU_UTILIZATION,
                                             SMApiConstants.Transformer.STAT_MIN),
            JobMonitorTransformer.MetricMeta('cpumax_{}', SMApiConstants.Transformer.METRIC_CPU_UTILIZATION,
                                             SMApiConstants.Transformer.STAT_MAX),

            JobMonitorTransformer.MetricMeta('memavg_{}', SMApiConstants.Transformer.METRIC_MEMORY_UTILIZATION,
                                             SMApiConstants.Transformer.STAT_AVG),
            JobMonitorTransformer.MetricMeta('memmin_{}', SMApiConstants.Transformer.METRIC_MEMORY_UTILIZATION,
                                             SMApiConstants.Transformer.STAT_MIN),
            JobMonitorTransformer.MetricMeta('memmax_{}', SMApiConstants.Transformer.METRIC_MEMORY_UTILIZATION,
                                             SMApiConstants.Transformer.STAT_MAX),
        ]

    def _describe_job(self):
        return self._sagemaker_client.describe_transform_job(TransformJobName=self._job_name)

    def _job_status(self, describe_response):
        return describe_response[SMApiConstants.Transformer.JOB_STATUS]

    def _job_start_time(self, describe_response):
        return describe_response.get(SMApiConstants.Transformer.START_TIME)

    def _job_end_time(self, describe_response):
        return describe_response.get(SMApiConstants.Transformer.END_TIME)

    def _report_online_metrics(self, describe_response):
        self._report_transform_job_metrics(describe_response,
                                           num_retries=JobMonitorTransformer.ONLINE_METRICS_FETCHING_NUM_RETRIES)

    def _report_final_metrics(self, describe_response):
        if self._host_metrics_fetched_successfully:
            self._logger.debug("Skipping final host metrics fetching!")
        else:
            self._logger.info("Trying to fetch final host metrics ... (#attempts: {})"
                              .format(JobMonitorTransformer.FINAL_METRICS_FETCHING_NUM_RETRIES))
            self._report_transform_job_metrics(describe_response,
                                               num_retries=JobMonitorTransformer.FINAL_METRICS_FETCHING_NUM_RETRIES)
            if not self._host_metrics_fetched_successfully:
                self._logger.error("Failed to fetch and report final host metrics!")

    def _report_transform_job_metrics(self, describe_response, num_retries):
        # No reason to start reading metrics before the job is actually starting
        if self._job_start_time(describe_response):
            job_instance_ids = self._get_transform_job_instance_ids(num_retries)
            if job_instance_ids:
                self._logger.info("Job instance ids: {}".format(job_instance_ids))
                metrics_data = self._get_transform_job_metrics(job_instance_ids, describe_response)
                Report.transform_job_metrics(self._job_name, metrics_data)
                self._host_metrics_fetched_successfully = True
            else:
                self._logger.info("Skip transform job host metrics reporting!")

    def _get_transform_job_instance_ids(self, num_retries):
        instance_ids = []
        for retry_index in range(num_retries):
            paginator = self._cloudwatch_client.get_paginator('list_metrics')
            response_iterator = paginator.paginate(Dimensions=[{'Name': SMApiConstants.Transformer.HOST_KEY}],
                                                   MetricName=SMApiConstants.Transformer.METRIC_CPU_UTILIZATION,
                                                   Namespace=SMApiConstants.Transformer.NAMESPACE)
            for response in response_iterator:
                # if self._logger.isEnabledFor(logging.DEBUG):
                #     self._logger.debug(pprint.pformat(response, indent=4))
                for metric in response[SMApiConstants.Transformer.LIST_METRICS_NAME]:
                    instance_id = metric[SMApiConstants.Transformer.LIST_METRICS_DIM][0][SMApiConstants.Transformer.LIST_METRICS_DIM_VALUE]
                    if instance_id.startswith(self._job_name):
                        instance_ids.append(instance_id)

            if instance_ids or retry_index == num_retries - 1:
                break

            time.sleep(JobMonitorTransformer.SLEEP_TIME_BETWEEN_METRICS_FETCHING_RETRIES_SEC)
            self._logger.debug("Another attempt to find job instance id! job name: {}, #attempt: {}"
                               .format(self._job_name, retry_index))

        if not instance_ids:
            self._logger.info("Couldn't find job instance id! job name: {}".format(self._job_name))

        return instance_ids

    def _get_transform_job_metrics(self, job_instance_ids, describe_response):
        start_time = self._job_start_time(describe_response)
        # Incrementing end time by 1 min since CloudWatch drops seconds before finding the logs.
        # This results in logs being searched in the time range in which the correct log line was not present.
        # Example - Log time - 2018-10-22 08:25:55
        #           Here calculated end time would also be 2018-10-22 08:25:55 (without 1 min addition)
        #           CW will consider end time as 2018-10-22 08:25 and will not be able to search the correct log.
        end_time = self._job_end_time(describe_response) + timedelta(minutes=1)
        d, r = divmod((end_time - start_time).total_seconds(), 60)
        period = int(d) * 60 + 60  # must be a multiplier of 60
        self._logger.debug("Start time: {}, end time: {}, period: {} sec".format(start_time, end_time, period))

        metric_data_queries = self._metric_data_queries(job_instance_ids, period)

        if self._logger.isEnabledFor(logging.DEBUG):
            self._logger.debug(pprint.pformat(metric_data_queries, indent=4))

        response = self._cloudwatch_client.get_metric_data(
            MetricDataQueries=metric_data_queries,
            StartTime=start_time,
            EndTime=end_time,
            ScanBy=SMApiConstants.Transformer.TIMESTAMP_ASC
        )

        if self._logger.isEnabledFor(logging.DEBUG):
            self._logger.debug(pprint.pformat(response, indent=4))

        return response[SMApiConstants.Transformer.METRICS_RESULTS]

    def _metric_data_queries(self, job_instance_ids, period):
        metric_data_queries = []
        for job_instance_id in job_instance_ids:
            inst_id = job_instance_id.split('-')[-1]
            for metric_meta in self._host_metrics_defs:
                query = {
                    'Id': metric_meta.id.format(inst_id),
                    'MetricStat': {
                        'Metric': {
                            'Namespace': SMApiConstants.Transformer.NAMESPACE,
                            'MetricName': metric_meta.metric_name,
                            'Dimensions': [
                                {
                                    'Name': SMApiConstants.Transformer.HOST_KEY,
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
