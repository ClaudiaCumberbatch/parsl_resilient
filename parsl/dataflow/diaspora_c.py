from datetime import datetime
import logging
import pprint
import time
import sqlite3
import signal
import random
import json
from typing import Any, Callable, Dict, List, Sequence, Tuple
from multiprocessing import Queue

from diaspora_event_sdk import KafkaConsumer
from kafka import TopicPartition

from parsl.utils import setproctitle
from parsl.executors.base import ParslExecutor
from parsl.multiprocessing import ForkProcess as mpForkProcess
from parsl.dataflow.taskrecord import TaskRecord


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

def choose_by_fail_num(logger, start_time) -> str:
    # read failure info from diaspora
    topic = "failure-info"
    logger.warning("Creating Kafka consumer for topic: {}".format(topic))
    # temporarily consuming all messages in failure-info, 
    # will change to consume only the messages from the start_time to end_time, i.e. now
    
    # consumer = KafkaConsumer(topic)
    consumer = KafkaConsumer(topic, auto_offset_reset="earliest")
    partition = 0
    topic_partition = TopicPartition(topic, partition)
    # start_offsets = consumer.offsets_for_times({topic_partition: int(start_time * 1000)})
    # start_offset = start_offsets[topic_partition].offset if start_offsets[topic_partition] else None
    # if start_offset:
    #     consumer.seek(topic_partition, start_offset)

    end_offsets = consumer.end_offsets([topic_partition])
    last_offset = end_offsets[topic_partition] - 1

    fail_executor = {}
    for message in consumer:
        logger.warning("Received message: {}".format(message))
        message_dict = json.loads(message.value.decode('utf-8'))
        executor = message_dict['task_executor']
        fail_executor[executor] = fail_executor.get(executor, 0) + 1
        if message.offset >= last_offset:
            break

    # choose the executor with the least failures
    logger.warning("Failure info: {}".format(fail_executor))
    min_fail = min(fail_executor.values())
    min_fail_executor = [k for k, v in fail_executor.items() if v == min_fail]
    return min_fail_executor

def choose_by_fail_type(logger, start_time, executors) -> str:
    # read resource info from diaspora
    topic = "radio-test"
    logger.warning("Creating Kafka consumer for topic: {}".format(topic))
    consumer = KafkaConsumer(topic)

    partition = 0
    topic_partition = TopicPartition(topic, partition)
    logger.warning(f"start time = {int(start_time * 1000)}")
    # One hour before real start time, only for test. If not set, consumer can't be invoked.
    tricky_start_time = int((start_time-3600*2) * 1000)
    logger.warning(f"tricky start time = {tricky_start_time}")
    start_offsets = consumer.offsets_for_times({topic_partition: tricky_start_time})
    start_offset = start_offsets[topic_partition].offset if start_offsets[topic_partition] else None
    logger.warning(f"start offset = {start_offset}")
    if start_offset:
        consumer.seek(topic_partition, start_offset)
    else:
        return [random.choice(list(executors.keys()))]
    end_offsets = consumer.end_offsets([topic_partition])
    last_offset = end_offsets[topic_partition] - 1
    logger.warning(f"last offset = {last_offset}")

    # count memory usage for each executor in each resource_monitoring_interval
    max_memory_by_pid = {}
    memory_usage_by_time_and_executor = {}
    for message in consumer:
        logger.warning("Received message: {}".format(message))
        message_dict = json.loads(message.value.decode('utf-8'))

        pid = message_dict['pid']
        logger.warning(f"pid = {pid}")
        timestamp = datetime.strptime(message_dict['timestamp'], '%Y-%m-%dT%H:%M:%S.%f').replace(microsecond=0)
        memory = message_dict['psutil_process_memory_percent']
        executor_label = message_dict['executor_label']
        key = (timestamp, executor_label)
        logger.warning(f"key = {key}, memory = {memory}")

        if pid not in max_memory_by_pid or max_memory_by_pid[pid] < memory:
            max_memory_by_pid[pid] = memory
            logger.warning(f"max_memory_by_pid = {max_memory_by_pid}")

        if memory_usage_by_time_and_executor.get(key, 0) < memory:
            previous_memory = max_memory_by_pid.get(pid, 0)
            if not memory_usage_by_time_and_executor.get(key, 0):
                memory_usage_by_time_and_executor[key] = memory
            else:
                memory_usage_by_time_and_executor[key] = memory_usage_by_time_and_executor.get(key, 0) - previous_memory + memory
            logger.warning(f"equation: {memory_usage_by_time_and_executor.get(key, 0)} - {previous_memory} + {memory}")
            logger.warning(f"memory_usage_by_time_and_executor = {memory_usage_by_time_and_executor}")

        if message.offset >= last_offset:
            break

    for key, total_memory in memory_usage_by_time_and_executor.items():
        logger.warning(f"Timestamp: {key[0]}, Executor: {key[1]}, Total Memory: {total_memory}")

    # switch to the executor with the least memory usage
    keys = list(memory_usage_by_time_and_executor.keys())
    logger.warning(f"keys = {keys}")
    if len(keys) > 1:
        res = [keys[0][1]]
    else:
        res = None
    return res


def choose_executor(executors: Dict[str, ParslExecutor], 
                    strategy: str, 
                    logging_level: int,
                    run_dir: str,
                    start_time: float,
                    task_record: TaskRecord) -> str:
    """Choose an executor based on the strategy.

    This function will return an executor label based on the strategy.
    """

    # set logger
    logger = start_file_logger('{}/resilience_module.log'.format(run_dir),
                            0,
                            level=logging_level)

    if strategy == "fail_num":
        label = choose_by_fail_num(logger, start_time, executors)[0]
    elif strategy == "fail_type":
        label = choose_by_fail_type(logger, start_time, executors)[0]
    else:
       label = random.choice(list(executors.keys()))

    logger.warning("Choosing executor: {}".format(label))
    return random.choice(list(executors.keys()))