import os
import time
import logging
import datetime
import parsl.monitoring.radios as radios
from functools import wraps

from parsl.multiprocessing import ForkProcess
from multiprocessing import Event, Barrier
from parsl.process_loggers import wrap_with_logs

from parsl.monitoring.message_type import MessageType
from typing import Any, Callable, Dict, List, Sequence, Tuple

logger = logging.getLogger(__name__)


def monitor_wrapper(f: Any,           # per app
                    args: Sequence,   # per invocation
                    kwargs: Dict,     # per invocation
                    x_try_id: int,    # per invocation
                    x_task_id: int,   # per invocation
                    monitoring_hub_url: str,   # per workflow
                    run_id: str,      # per workflow
                    logging_level: int,  # per workflow
                    sleep_dur: float,  # per workflow
                    radio_mode: str,   # per executor
                    monitor_resources: bool,  # per workflow
                    run_dir: str) -> Tuple[Callable, Sequence, Dict]:
    """Wrap the Parsl app with a function that will call the monitor function and point it at the correct pid when the task begins.
    """

    @wraps(f)
    def wrapped(*args: List[Any], **kwargs: Dict[str, Any]) -> Any:
        task_id = kwargs.pop('_parsl_monitoring_task_id')
        try_id = kwargs.pop('_parsl_monitoring_try_id')
        terminate_event = Event()
        # Send first message to monitoring router
        send_first_message(try_id,
                           task_id,
                           monitoring_hub_url,
                           run_id,
                           radio_mode,
                           run_dir)

        try:
            return f(*args, **kwargs)
        finally:
            send_last_message(try_id,
                                task_id,
                                monitoring_hub_url,
                                run_id,
                                radio_mode, run_dir)

    new_kwargs = kwargs.copy()
    new_kwargs['_parsl_monitoring_task_id'] = x_task_id
    new_kwargs['_parsl_monitoring_try_id'] = x_try_id

    return (wrapped, args, new_kwargs)


@wrap_with_logs
def send_first_message(try_id: int,
                       task_id: int,
                       monitoring_hub_url: str,
                       run_id: str, radio_mode: str, run_dir: str) -> None:
    send_first_last_message(try_id, task_id, monitoring_hub_url, run_id,
                            radio_mode, run_dir, False)


@wrap_with_logs
def send_last_message(try_id: int,
                      task_id: int,
                      monitoring_hub_url: str,
                      run_id: str, radio_mode: str, run_dir: str) -> None:
    send_first_last_message(try_id, task_id, monitoring_hub_url, run_id,
                            radio_mode, run_dir, True)


def send_first_last_message(try_id: int,
                            task_id: int,
                            monitoring_hub_url: str,
                            run_id: str, radio_mode: str, run_dir: str,
                            is_last: bool) -> None:
    return
    import platform
    import os

    radio: radios.MonitoringRadio
    radio = radios.get_monitoring_radio(monitoring_hub_url, task_id, radio_mode, run_dir)

    msg = (MessageType.RESOURCE_INFO,
           {'run_id': run_id,
            'try_id': try_id,
            'task_id': task_id,
            'hostname': platform.node(),
            'block_id': os.environ.get('PARSL_WORKER_BLOCK_ID'),
            'first_msg': not is_last,
            'last_msg': is_last,
            'timestamp': datetime.datetime.now(),
            'pid': os.getpid()
            }
    )
    radio.send(msg)
    if isinstance(radio, radios.DiasporaRadio):
        radio.flush()
    return

