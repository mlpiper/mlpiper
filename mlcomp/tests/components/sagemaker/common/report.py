from datetime import timedelta

from parallelm.mlops import mlops
from parallelm.mlops.stats.table import Table


class Report(object):
    _last_metric_values = {}

    @staticmethod
    def job_status(job_name, running_time_sec, status):
        Report._last_metric_values[job_name] = status
        tbl = Table().name("SageMaker Job Status").cols(["Running Time (sec)", "Status"])
        tbl.add_row(job_name, [str(timedelta(seconds=running_time_sec)), status])
        mlops.set_stat(tbl)

    @staticmethod
    def job_metric(metric_name, value):
        last_value = Report._last_metric_values.get(metric_name)
        if last_value is None or last_value != value:
            Report._last_metric_values[metric_name] = value
            mlops.set_stat(metric_name, value)
