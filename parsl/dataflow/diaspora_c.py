import logging
import pprint
import time
import sqlite3
import signal
from typing import Any, Callable, Dict, List, Sequence, Tuple
from multiprocessing import Queue

from diaspora_event_sdk import KafkaConsumer
from kafka import TopicPartition

from parsl.utils import setproctitle


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

def diaspora_consumer_loop(logging_level: int,
            run_dir: str,
            topic: str,
            terminate_event: Any,
            start_time: float,
            instruction_queue: Queue) -> None:
    """Start the Diaspora consumer loop.

    This function starts the Diaspora consumer loop which is responsible for
    consuming messages from the message queue and processing them.
    """
    setproctitle("parsl: diaspora consumer")
    logger = start_file_logger('{}/diaspora_consumer.log'.format(run_dir),
                                   0,
                                   level=logging_level)
    logger.warning("Starting diaspora consumer!")

    logger.warning("Creating Kafka consumer for topic: {}".format(topic))
    consumer = KafkaConsumer(topic)

    partition = 0
    topic_partition = TopicPartition(topic, partition)
    offsets = consumer.offsets_for_times({topic_partition: int(start_time * 1000)})
    offset = offsets[topic_partition].offset if offsets[topic_partition] else None
    if offset:
        consumer.seek(topic_partition, offset)

    while not terminate_event.is_set():
        for message in consumer:
            logger.warning("Received message: {}".format(message))
            instruction_queue.put(message)

    # for message in consumer:
    #     if message.key is None:
    #         continue
    #     message_key_str = message.key.decode('utf-8')
    #     # print(f"message key: {message_key_str}")
    #     # break
    #     if message_key_str in run_ids:
    #         record_per_workflow[message_key_str] = record_per_workflow.get(message_key_str, 0) + 1
    #         # print(record_per_workflow)