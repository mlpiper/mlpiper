from time import gmtime, strftime

from parallelm.mlops import mlops
from parallelm.mlops.stats.table import Table


class Report(object):
    _last_metric_values = {}

    @staticmethod
    def job_status(job_name, status):
        last_value = Report._last_metric_values.get(job_name)
        if last_value is None or last_value != status:
            Report._last_metric_values[job_name] = status
            tbl = Table().name("SageMaker Job Status").cols(["Job Name", "Update Time", "Status"])
            tbl.add_row([job_name, strftime("%Y/%m/%d %H:%M:%S", gmtime()), status])
            mlops.set_stat(tbl)

    @staticmethod
    def job_metric(metric_name, value):
        last_value = Report._last_metric_values.get(metric_name)
        if last_value is None or last_value != value:
            Report._last_metric_values[metric_name] = value
            mlops.set_stat(metric_name, value)
