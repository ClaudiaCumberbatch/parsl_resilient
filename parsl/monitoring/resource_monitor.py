import os
import time
import logging
import datetime
import platform
import psutil
import getpass
import json
import parsl.monitoring.radios as radios
from functools import wraps

from parsl.multiprocessing import ForkProcess
from multiprocessing import Event, Barrier, Queue
from parsl.process_loggers import wrap_with_logs
from parsl.utils import setproctitle

from parsl.monitoring.message_type import MessageType
from parsl.monitoring.energy.base import NodeEnergyMonitor
from typing import Any, Callable, Dict, List, Sequence, Tuple

try:
    import performance_features
except ImportError:
    _perf_counters_enabled = False
else:
    _perf_counters_enabled = True

# these values are simple to log. Other information is available in special formats such as memory below.
simple = ["cpu_num", 'create_time', 'cwd', 'exe', 'memory_percent', 'nice', 'name', 'num_threads', 'pid', 'ppid', 'status', 'username']
# values that can be summed up to see total resources used by task process and its children
summable_values = ['memory_percent', 'num_threads']
# perfomance counters read from performance features that can be used to monitor energy
events= [['UNHALTED_CORE_CYCLES'], ['UNHALTED_REFERENCE_CYCLES'], ['LLC_MISSES'], ['INSTRUCTION_RETIRED']]


def measure_resource_utilization(run_id: str,
                           block_id: int,
                           proc: psutil.Process, 
                           profiler: Any = None):

    # children_user_time = {}  # type: Dict[int, float]
    # children_system_time = {}  # type: Dict[int, float]

    d = dict()
    d["run_id"] = run_id
    d["block_id"] = block_id
    d["pid"] = proc.info["pid"]
    d['hostname'] = platform.node()
    d['first_msg'] = False
    d['last_msg'] = False
    d['timestamp'] = datetime.datetime.now()
    d["psutil_process_name"] = proc.info["name"].encode('utf-8','surrogatepass').decode('utf-8')
    d["psutil_process_ppid"] = proc.info["ppid"]

    if not profiler:
        children_user_time = {}
        children_system_time = {}

        d.update({"psutil_process_" + str(k): v for k, v in proc.as_dict().items() if k in simple})
        d["psutil_cpu_count"] = psutil.cpu_count()
        d['psutil_process_memory_virtual'] = proc.memory_info().vms
        d['psutil_process_memory_resident'] = proc.memory_info().rss
        d['psutil_process_time_user'] = proc.cpu_times().user
        d['psutil_process_time_system'] = proc.cpu_times().system
        try:
            d['psutil_process_disk_write'] = proc.io_counters().write_chars
            d['psutil_process_disk_read'] = proc.io_counters().read_chars
        except Exception:
            # occasionally pid temp files that hold this information are unvailable to be read so set to zero
            logging.exception("Exception reading IO counters for main process. Recorded IO usage may be incomplete", exc_info=True)
            d['psutil_process_disk_write'] = 0
            d['psutil_process_disk_read'] = 0

        logging.debug("getting children")
        children = proc.children(recursive=True)
        d['psutil_process_children_count'] = len(children)
        logging.debug("got children")

        for child in children:
            for k, v in child.as_dict(attrs=summable_values).items():
                d['psutil_process_' + str(k)] += v
            child_user_time = child.cpu_times().user
            child_system_time = child.cpu_times().system
            children_user_time[child.pid] = child_user_time
            children_system_time[child.pid] = child_system_time
            d['psutil_process_memory_virtual'] += child.memory_info().vms
            d['psutil_process_memory_resident'] += child.memory_info().rss
            try:
                d['psutil_process_disk_write'] += child.io_counters().write_chars
                d['psutil_process_disk_read'] += child.io_counters().read_chars
            except Exception:
                # occassionally pid temp files that hold this information are unvailable to be read so add zero
                logging.exception("Exception reading IO counters for child {k}. Recorded IO usage may be incomplete".format(k=k), exc_info=True)
                d['psutil_process_disk_write'] += 0
                d['psutil_process_disk_read'] += 0
        total_children_user_time = 0.0
        for child_pid in children_user_time:
            total_children_user_time += children_user_time[child_pid]
        total_children_system_time = 0.0
        for child_pid in children_system_time:
            total_children_system_time += children_system_time[child_pid]
        d['psutil_process_time_user'] += total_children_user_time
        d['psutil_process_time_system'] += total_children_system_time

    else:
        event_counters = profiler.read_events()
        event_counters = profiler._Profiler__format_data([event_counters,])

        # Send event counters
        d['perf_unhalted_core_cycles'] = event_counters[0][0]
        d['perf_unhalted_reference_cycles'] = event_counters[0][1]
        d['perf_llc_misses'] = event_counters[0][2]
        d['perf_instructions_retired'] = event_counters[0][3]
    
    logging.debug("sending message")
    return d

def measure_energy_use(energy_monitor: NodeEnergyMonitor,
                       run_id: str,
                       block_id: str,
                       sleep_dur: float):
    report = energy_monitor.report()
    d = dict()
    d["total_energy"] = report.total_energy
    d["devices"] = json.dumps(report.dict()["devices"])
    d["run_id"] = run_id
    d["block_id"] = block_id
    d["resource_monitoring_interval"] = sleep_dur
    d["hostname"] = platform.node()
    d["timestamp"] = datetime.datetime.now()
    d["duration"] = report.end_time - report.start_time
    return d
    

def start_file_logger(filename, rank, name=__name__, level=logging.DEBUG, format_string=None):
    """Add a stream log handler.

    Args:
        - filename (string): Name of the file to write logs to
        - name (string): Logger name
        - level (logging.LEVEL): Set the logging level.
        - format_string (string): Set the format string

    Returns:
       -  None
    """
    if format_string is None:
        format_string = "%(asctime)s.%(msecs)03d %(name)s:%(lineno)d " \
                        "%(process)d %(threadName)s " \
                        "[%(levelname)s]  %(message)s"

    logger = logging.getLogger(name)
    if logger.hasHandlers():
        logger.handlers = []

    logger.setLevel(level)
    handler = logging.FileHandler(filename)
    handler.setLevel(level)
    formatter = logging.Formatter(format_string, datefmt='%Y-%m-%d %H:%M:%S')
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    
    return logger

def resource_monitor_loop(executor_label: str,
            monitoring_hub_url: str,
            manager_id: str,
            run_id: str,
            radio_mode: str,
            logging_level: int,
            sleep_dur: float,
            run_dir: str,
            block_id: int,
            energy_monitor: None | NodeEnergyMonitor,
            terminate_event: Any,
            procQueue: Queue) -> None:  # cannot be Event/Barrier because of multiprocessing type weirdness.
    """Monitors the Parsl task's resources by pointing psutil to the task's pid and watching it and its children.
    """

    setproctitle("parsl: resource monitor")
    logger = start_file_logger('{}/block-{}/{}/resource_monitor.log'.format(run_dir, block_id, manager_id),
                                   0,
                                   level=logging.DEBUG)
    logger.warning("Starting resource monitor!")

    radio: radios.MonitoringRadio
    radio = radios.get_monitoring_radio(monitoring_hub_url, manager_id, radio_mode, run_dir)

    logger.info("start of monitor")
    logger.info("Energy Monitor: {}".format(energy_monitor))

    user_name = getpass.getuser()

    profilers = dict()
    
    next_send = time.time()

    def check_queue_contents(q, target):
        '''
        Check if the target is in the queue and return True if it is, False otherwise.
        '''
        temp_list = []
        res = False
        while not q.empty():
            item = q.get()
            temp_list.append(item)
            if item == target:
                res = True
                break
        
        for item in temp_list:
            q.put(item)
        return res

    while not terminate_event.is_set():
        logger.debug("start of monitoring loop")
        for proc in psutil.process_iter(['pid', 'username', 'name', 'ppid']): # traverse
            if proc.info["username"] != user_name or proc.info["pid"] == os.getpid() or not check_queue_contents(procQueue, proc.info["pid"]):
            # if proc.info["username"] != user_name or proc.info["pid"] == os.getpid():
                continue

            if _perf_counters_enabled and proc.info["pid"] not in profilers:
                try:
                    profiler = performance_features.Profiler(pid=proc.info["pid"], events_groups=events)
                    profiler._Profiler__initialize()
                    profiler.reset_events()
                    profiler.enable_events()
                    profilers[proc.info["pid"]] = profiler
                except Exception:
                    logger.exception("Exception starting performance counter profiler", exc_info=True)
                    profilers[proc.info["pid"]] = None
                else:
                    logger.debug("Started performance counter for process {}".format(proc.info["pid"]))
            
            profiler = profilers.get(proc.info["pid"])

            try:
                d = measure_resource_utilization(run_id, block_id, proc, profiler)
                d["executor_label"] = executor_label
                # logger.debug("Sending intermediate resource message {}".format(d))
                # for performance evaluation
                # start = time.time()
                radio.send((MessageType.RESOURCE_INFO, d))
                # end = time.time()
                # logger.error(f"start = {start}, end = {end}, Sent message in {end-start} seconds")
            except Exception:
                logger.exception("Exception getting the resource usage. Not sending usage to Hub", exc_info=True)

        if energy_monitor:
            try:
                d = measure_energy_use(energy_monitor, run_id, block_id, sleep_dur)
                logger.debug("Sending energy message")
                # radio.send((MessageType.ENERGY_INFO, d))
            except Exception:
                logger.exception("Exception getting the resource usage. Not sending usage to Hub", exc_info=True)
        
        logger.debug("sleeping")
        terminate_event.wait(max(0, next_send - time.time()))
        next_send += sleep_dur

    if isinstance(radio, radios.DiasporaRadio):
        radio.flush()