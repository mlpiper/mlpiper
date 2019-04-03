import json
import logging
import os
import socket

from parallelm.components.restful.constants import StatsConstants, RestfulConstants
from parallelm.components.restful.metric import Metric, MetricRelation
from parallelm.components.restful.uwsgi_stats_snapshot import UwsiStatsSnapshot

mlops_loaded = False
try:
    from parallelm.mlops import mlops
    from parallelm.mlops.stats_category import StatCategory
    from parallelm.mlops.stats.bar_graph import BarGraph
    from parallelm.mlops.stats.multi_line_graph import MultiLineGraph
    from parallelm.mlops.stats.table import Table
    from parallelm.mlops.predefined_stats import PredefinedStats
    mlops_loaded = True
except ImportError as e:
    print("Note: 'mlops' was not loaded: " + str(e))


class UwsgiStatistics(object):
    STATS_JSON_MAX_SIZE_BYTES = 64 * 2048  # max 64 cores, 2k per core

    def __init__(self, reporting_interval_sec, target_path, sock_filename, logger, stats_path=None):
        self._logger = logger
        self._reporting_interval_sec = reporting_interval_sec
        self._server_address = os.path.join(target_path, sock_filename)
        self._stats_sock = None

        self._curr_stats_snapshot = None
        self._prev_stats_snapshot = None
        self._prev_metrics_snapshot = None
        self._stats_path = stats_path

    def report(self):
        raw_stats = self._read_raw_statistics()
        if not raw_stats:
            return

        self._curr_stats_snapshot = UwsiStatsSnapshot(raw_stats, self._prev_stats_snapshot)

        if mlops_loaded:
            if self._logger.isEnabledFor(logging.DEBUG):
                self._logger.debug(self._curr_stats_snapshot)

            if self._curr_stats_snapshot.should_report_requests_per_window_time(self._prev_stats_snapshot):
                self._report_requests_per_window_time()

            if self._curr_stats_snapshot.should_report_worker_status(self._prev_stats_snapshot):
                self._report_acc_requests_and_status()

            if self._curr_stats_snapshot.should_report_average_response_time(self._prev_stats_snapshot):
                self._report_avg_response_time_metrics()

            if self._curr_stats_snapshot.should_report_metrics_accumulation(self._prev_stats_snapshot):
                self._report_metrics_accumulation()

            if self._curr_stats_snapshot.should_report_metrics_per_time_window(self._prev_stats_snapshot):
                self._report_metrics_per_time_window()

        else:
            self._logger.info(self._curr_stats_snapshot)

        self._logger.debug("Stats path {}".format(self._stats_path))
        if self._stats_path is not None:
            self._write_json(self._stats_path)

        self._prev_stats_snapshot = self._curr_stats_snapshot

    def _write_json(self, file_path):
        stat_map = {}
        stat_map[RestfulConstants.STATS_SYSTEM] = mlops.get_stats_map()

        self._logger.debug("Writing stats {}".format(stat_map[RestfulConstants.STATS_SYSTEM].keys()))
        with open(file_path, 'w') as output:
            output.write(json.dumps(stat_map))
            output.close()

    def _read_raw_statistics(self):
        sock = self._setup_stats_connection()
        if sock:
            try:
                data = sock.recv(UwsgiStatistics.STATS_JSON_MAX_SIZE_BYTES)
                return json.loads(data.decode('utf-8'))
            except ValueError as e:
                self._logger.error("Invalid statistics json format! {}, data:\n{}\n".format(e.message, data))
            finally:
                if sock:
                    sock.close()
        return None

    def _setup_stats_connection(self):
        if self._logger.isEnabledFor(logging.DEBUG):
            self._logger.debug("Connecting to uWSGI statistics server via unix socket: {}"
                               .format(self._server_address))

        stats_sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        try:
            stats_sock.connect(self._server_address)
        except socket.error as ex:
            self._logger.warning("Failed to open connection to uWSI statistics server! {}".format(ex))
            return None
        return stats_sock

    def _report_requests_per_window_time(self):
        self._logger.debug("Reporting about requests per window ...")
        mlops.set_stat(StatsConstants.REQS_PER_WINDOW_TIME_GRAPH_TITLE
                       .format(self._reporting_interval_sec),
                       self._curr_stats_snapshot.total_requests_diff)

    def _report_acc_requests_and_status(self):
        self._logger.debug("Reporting about workers requests & status ...")
        try:
            predict_reqs = self._curr_stats_snapshot.total_requests - \
                           self._curr_stats_snapshot.uwsgi_pm_metric_by_name(PredefinedStats.PM_STAT_REQUESTS)
            mlops.set_stat("Number of Predict Requests", predict_reqs)
        except:
            self._logger.error("Failed to retrieve pm stat requests")
            predict_reqs = self._curr_stats_snapshot.total_requests

        tbl = Table().name(StatsConstants.ACC_REQS_TABLE_NAME).cols([StatsConstants.ACC_REQS_NUM_REQS_COL_NAME,
                                                                     StatsConstants.ACC_REQS_STATUS_COL_NAME])
        for col, value, status in self._curr_stats_snapshot.sorted_worker_stats:
            tbl.add_row(col, [value, status])

        tbl.add_row(StatsConstants.ACC_REQS_LAST_ROW_NAME, [self._curr_stats_snapshot.total_requests, "---"])
        mlops.set_stat(tbl)

        mlops.set_stat(PredefinedStats.WORKER_STATS, len(self._curr_stats_snapshot.worker_ids))

    def _report_avg_response_time_metrics(self):
        self._logger.debug("Reporting about workers average response time ...")
        tbl = Table().name(StatsConstants.AVG_RESP_TIME_TABLE_NAME)\
                     .cols([StatsConstants.AVG_RESP_TIME_COL_NAME])
        for col, rt in self._curr_stats_snapshot.avg_workers_response_time:
            tbl.add_row(col, rt)
        mlops.set_stat(tbl)

    def _report_metrics_accumulation(self):
        self._logger.debug("Reporting metrics accumulation ...")
        self._report_metrics_collection(self._curr_stats_snapshot.uwsgi_pm_metrics_accumulation)

    def _report_metrics_per_time_window(self):
        self._logger.debug("Reporting metrics per time window ...")
        self._report_metrics_collection(self._curr_stats_snapshot.uwsgi_pm_metrics_per_window)

    def _report_metrics_collection(self, metrics):
        for name, value in metrics.items():
            metric_meta = Metric.metric_by_name(name)
            self._logger.debug("Reporting metrics ... {}".format(metric_meta))
            self._logger.info("Reporting metrics ... {}".format(name))
            if not metric_meta.hidden:
                if metric_meta.metric_relation == MetricRelation.BAR_GRAPH:
                    self._report_bar_graph_metric(metric_meta, metrics)
                else:
                    mlops.set_stat(metric_meta.title, value)

    def _report_bar_graph_metric(self, metric_meta, metrics):
        cols = []
        data = []
        for related_m, bar_name in metric_meta.related_metric:
            cols.append(bar_name)
            data.append(metrics[related_m.metric_name])

        if not all(v == 0 for v in data) or not metric_meta.metric_already_displayed:
            metric_meta.metric_already_displayed = True
            mlt = BarGraph().name(metric_meta.title).cols(cols).data(data)
            mlops.set_stat(mlt)
