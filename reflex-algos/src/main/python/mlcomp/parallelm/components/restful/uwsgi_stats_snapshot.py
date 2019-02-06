
class UwsiStatsSnapshot(object):
    def __init__(self, raw_stats, prev_stats_snapshot):
        self._raw_stats = raw_stats
        self._total_requests = None
        self._total_requests_diff = None
        self._sorted_worker_stats = None
        self._avg_rt = None
        self._extract_relevant_stats(prev_stats_snapshot)

    def _extract_relevant_stats(self, prev_stats_snapshot):
        self._avg_rt = sorted([("wid-{:02d}".format(w["id"]), [w["avg_rt"]]) for w in self._raw_stats["workers"]
                               if w["id"] != 0])

        worker_requests = {"wid-{:02d}".format(w["id"]):
                               (w["requests"], w["status"].encode('utf8')) for w in self._raw_stats["workers"]
                               if w["id"] != 0}

        self._sorted_worker_stats = sorted([(k, v[0], v[1].decode()) for k, v in worker_requests.items()])

        self._total_requests = sum(v for _, v, _ in self._sorted_worker_stats)

        self._total_requests_diff = self._total_requests - prev_stats_snapshot.total_requests \
            if prev_stats_snapshot is not None else self._total_requests

    def __str__(self):
        return "Total_requests: {}, diff-requests: {}, requests + status: {}, avg response time: {}".format(
            self.total_requests,
            self.total_requests_diff,
            self.sorted_worker_stats,
            self.avg_workers_response_time)

    @property
    def total_requests(self):
        return self._total_requests

    @property
    def total_requests_diff(self):
        return self._total_requests_diff

    @property
    def sorted_worker_stats(self):
        return self._sorted_worker_stats

    @property
    def avg_workers_response_time(self):
        return self._avg_rt

    def should_report_requests_per_window_time(self, stats_snapshot):
        return stats_snapshot is None or (self.total_requests_diff != stats_snapshot.total_requests_diff)

    def should_report_average_response_time(self, stats_snapshot):
        return stats_snapshot is None or (self.avg_workers_response_time != stats_snapshot.avg_workers_response_time)

    def should_report_worker_status(self, stats_snapshot):
        return stats_snapshot is None or (self.sorted_worker_stats != stats_snapshot.sorted_worker_stats)
