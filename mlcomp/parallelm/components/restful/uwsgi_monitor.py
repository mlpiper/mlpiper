"""
For internal use only. The WsgiMonitor is designed to monitor the standard output/error of the 'uWSGI'
processes, as well as reading the statistics from the uwsgi master process.
the
"""
import os
import select
import time
import threading
import traceback

from parallelm.common.base import Base
from parallelm.common.mlcomp_exception import MLCompException
from parallelm.common.buff_to_lines import BufferToLines
from parallelm.components.restful.constants import UwsgiConstants, ComponentConstants, SharedConstants
from parallelm.components.restful.uwsgi_statistics import UwsgiStatistics

mlops_loaded = False
try:
    from parallelm.mlops import mlops
    from parallelm.mlops.stats_category import StatCategory
    from parallelm.mlops.stats.multi_line_graph import MultiLineGraph
    from parallelm.mlops.stats.table import Table
    mlops_loaded = True
except ImportError as e:
    print("Note: 'mlops' was not loaded: " + str(e))


class WsgiMonitor(Base):
    SUCCESS_RUN_INDICATION_MSG = "uwsgi entry point run successfully"
    STATS_JSON_MAX_SIZE_BYTES = 64 * 2048  # max 64 cores, 2k per core

    def __init__(self, ml_engine, monitor_info, shared_conf, uwsgi_entry_point_conf):
        super(self.__class__, self).__init__()
        self.set_logger(ml_engine.get_engine_logger(self.logger_name()))

        self._stats_reporting_interval_sec = uwsgi_entry_point_conf[ComponentConstants.STATS_REPORTING_INTERVAL_SEC]

        self._stats = None
        if not shared_conf.get(SharedConstants.STANDALONE):
            self._stats = UwsgiStatistics(self._stats_reporting_interval_sec, shared_conf["target_path"],
                                          shared_conf["stats_sock_filename"], self._logger,
                                          shared_conf["stats_path_filename"])

        self._proc = None
        self._monitor_info = monitor_info
        self._shared_conf = shared_conf

        (self._stdout_pipe_r, self._stdout_pipe_w) = os.pipe()
        (self._stderr_pipe_r, self._stderr_pipe_w) = os.pipe()

    @property
    def stdout_pipe_w(self):
        return self._stdout_pipe_w

    @property
    def stderr_pipe_w(self):
        return self._stderr_pipe_w

    def set_proc_for_monitoring(self, proc):
        self._proc = proc

    def verify_proper_startup(self):
        # Important note: the following code snippet assumes that the application's startup is not
        # demonized and consequently the logs can be read. Therefor, it is important to use
        # the 'daemonize2' option in 'uwsgi.ini' file
        self._monitor_uwsgi_proc(stop_msg=WsgiMonitor.SUCCESS_RUN_INDICATION_MSG)

    def start(self):
        if not self._proc:
            raise MLCompException("uWSGI process was not setup for monitoring!")

        th = threading.Thread(target=self._run)
        self._monitor_info[UwsgiConstants.MONITOR_THREAD_KEY] = th

        th.start()

    def _run(self):
        self._logger.info("Starting logging monitoring in the background ...")
        try:
            self._monitor_uwsgi_proc()
        except:
            self._monitor_info[UwsgiConstants.MONITOR_ERROR_KEY] = traceback.format_exc()
        finally:
            self._logger.info("Exited logging monitoring!")

    def _monitor_uwsgi_proc(self, stop_msg=None):
        try:
            monitor_stats = not stop_msg
            block_size = 2048

            stderr_fd = 2

            stdout_buff2lines = BufferToLines()
            stderr_buff2lines = BufferToLines()

            keep_reading = True
            last_stats_read = time.time()
            while keep_reading:

                read_fs = [self._stdout_pipe_r, self._stderr_pipe_r]

                # Sleep the exact time left within a 1 sec interval
                if monitor_stats:
                    sleep_time = self._stats_reporting_interval_sec - (time.time() - last_stats_read)
                    if sleep_time < 0:
                        sleep_time = 0
                else:
                    sleep_time = self._stats_reporting_interval_sec

                readable_fd = select.select(read_fs, [], [], sleep_time)[0]

                if monitor_stats:
                    wakeup_time = time.time()
                    if wakeup_time - last_stats_read > self._stats_reporting_interval_sec:
                        last_stats_read = wakeup_time
                        if self._stats:
                            self._stats.report()

                if readable_fd:
                    for pipe in readable_fd:
                        if pipe is self._stdout_pipe_r:
                            buff = os.read(pipe, block_size)
                            stdout_buff2lines.add(buff)
                            for line in stdout_buff2lines.lines():
                                print(line)

                            if stop_msg and stop_msg.encode() in buff:
                                keep_reading = False

                        if pipe is self._stderr_pipe_r:
                            buff = os.read(pipe, block_size)
                            stderr_buff2lines.add(buff)
                            for line in stderr_buff2lines.lines():
                                os.write(stderr_fd, (line + '\n').encode())
                else:
                    rc = self._proc.poll()
                    if rc is not None:
                        if rc != 0:
                            raise MLCompException("Error in 'uwsgi' server! rc: {}".format(rc))
                        break

        except MLCompException as e:
            self._cleanup()
            raise e

    def _cleanup(self):
        if self._stdout_pipe_r:
            os.close(self._stdout_pipe_r)
        if self._stdout_pipe_w:
            os.close(self._stdout_pipe_w)
        if self._stderr_pipe_r:
            os.close(self._stderr_pipe_r)
        if self._stderr_pipe_w:
            os.close(self._stderr_pipe_w)
