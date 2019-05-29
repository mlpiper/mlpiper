import abc
from datetime import datetime
import logging
import pprint
import pytz
import time

from parallelm.common.mlcomp_exception import MLCompException
from parallelm.extra.sagemaker.monitor.report import Report
from parallelm.extra.sagemaker.monitor.sm_api_constants import SMApiConstants


class JobMonitorBase(object):
    MONITOR_INTERVAL_SEC = 10.0

    def __init__(self, sagemaker_client, job_name, logger):
        self._logger = logger
        self._sagemaker_client = sagemaker_client
        self._job_name = job_name
        self._on_complete_callback = None

    def monitor(self):
        self._logger.info("Monitoring job ... {}".format(self._job_name))
        while True:
            response = self._describe_job()
            if self._logger.isEnabledFor(logging.DEBUG):
                self._logger.debug(pprint.pformat(response, indent=4))

            status = self._job_status(response)
            running_time_sec = self._total_running_time_sec(response)
            billing_time_sec = self._billing_time_sec(response)
            Report.job_status(self._job_name, running_time_sec, billing_time_sec, status)

            self._report_online_metrics(response)

            if status == SMApiConstants.JOB_COMPLETED:
                self._report_final_metrics(response)
                self._logger.info("Job '{}' completed!".format(self._job_name))
                if self._on_complete_callback:
                    self._on_complete_callback(response)
                break
            elif status == SMApiConstants.JOB_FAILED:
                msg = "Job '{}' failed! message: {}"\
                    .format(self._job_name, response[SMApiConstants.FAILURE_REASON])
                self._logger.error(msg)
                raise MLCompException(msg)
            elif status != SMApiConstants.JOB_IN_PROGRESS:
                self._logger.warning("Unexpected job status! job-name: {}, status: {}"
                                     .format(self._job_name, status))

            self._logger.info("Job '{}' is still running ... {} sec"
                              .format(self._job_name, running_time_sec))
            time.sleep(JobMonitorBase.MONITOR_INTERVAL_SEC)

    def set_on_complete_callback(self, on_complete_callback):
        # The prototype of the callback is 'callback(describe_response)'
        self._on_complete_callback = on_complete_callback
        return self

    def _total_running_time_sec(self, describe_response):
        create_time = self._job_create_time(describe_response)
        if create_time is None:
            return None

        end_time = self._job_end_time(describe_response)
        if end_time:
            return (end_time - create_time).total_seconds()
        else:
            return (datetime.now(pytz.UTC) - create_time).total_seconds()

    def _billing_time_sec(self, response):
        start_time = self._job_start_time(response)
        end_time = self._job_end_time(response)
        if start_time and end_time:
            return (end_time - start_time).total_seconds()
        else:
            return None

    def _job_create_time(self, describe_response):
        return describe_response.get(SMApiConstants.CREATE_TIME)

    @abc.abstractmethod
    def _describe_job(self):
        pass

    @abc.abstractmethod
    def _job_start_time(self, describe_response):
        pass

    @abc.abstractmethod
    def _job_end_time(self, describe_response):
        pass

    @abc.abstractmethod
    def _job_status(self, describe_response):
        pass

    @abc.abstractmethod
    def _report_online_metrics(self, describe_response):
        pass

    @abc.abstractmethod
    def _report_final_metrics(self, describe_response):
        pass
