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

def choose_by_fail_type(logger, start_time) -> str:
    # read resource info from diaspora
    topic = "radio-test"
    logger.warning("Creating Kafka consumer for topic: {}".format(topic))
    consumer = KafkaConsumer(topic)

    partition = 0
    topic_partition = TopicPartition(topic, partition)
    start_offsets = consumer.offsets_for_times({topic_partition: int(start_time * 1000)})
    start_offset = start_offsets[topic_partition].offset if start_offsets[topic_partition] else None
    if start_offset:
        consumer.seek(topic_partition, start_offset)
    end_offsets = consumer.end_offsets([topic_partition])
    last_offset = end_offsets[topic_partition] - 1

    # count memory usage for each executor
    cnt_mem = {}
    for message in consumer:
        logger.warning("Received message: {}".format(message))
        message_dict = json.loads(message.value.decode('utf-8'))
        executor = message_dict['executor_label']
        mem = message_dict['psutil_process_memory_percent']
        cnt_mem[executor] = cnt_mem.get(executor, 0) + mem
        if message.offset >= last_offset:
            break
    # switch to the executor with the least memory usage
    logger.warning("Memory usage info: {}".format(cnt_mem))
    min_mem = min(cnt_mem.values())
    min_mem_executor = [k for k, v in cnt_mem.items() if v == min_mem]
    return min_mem_executor


def choose_executor(executors: Dict[str, ParslExecutor], 
                    strategy: str, 
                    logging_level: int,
                    run_dir: str,
                    start_time: float,) -> str:
    """Choose an executor based on the strategy.

    This function will return an executor label based on the strategy.
    """

    # set logger
    logger = start_file_logger('{}/resilience_module.log'.format(run_dir),
                            0,
                            level=logging_level)

    if strategy == "fail_num":
        label = choose_by_fail_num(logger, start_time)[0]
    elif strategy == "fail_type":
        label = choose_by_fail_type(logger, start_time)[0]
    else:
       label = random.choice(list(executors.keys()))

    logger.warning("Choosing executor: {}".format(label))
    return random.choice(list(executors.keys()))