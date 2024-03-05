from diaspora_event_sdk import block_until_ready
assert block_until_ready()

from diaspora_event_sdk import KafkaConsumer
import json
import pprint
from datetime import datetime

def date_to_timestamp(specified_date):
    date_obj = datetime.strptime(specified_date, "%Y-%m-%d")
    timestamp = datetime.timestamp(date_obj)
    return timestamp

if __name__ == '__main__':
    specified_date = "2024-03-01"
    specified_timestamp = date_to_timestamp(specified_date)

    topic = 'radio-test'
    # consumer = KafkaConsumer(topic)
    consumer = KafkaConsumer(topic, auto_offset_reset='earliest')
    for record in consumer:
        if record.timestamp < specified_timestamp:
            break
        value = json.loads(record.value.decode('utf-8'))
        if 'psutil_process_memory_percent' in value.keys() and value['psutil_process_memory_percent'] > 0.5:
            pprint.pprint(value['psutil_process_memory_percent'])

        