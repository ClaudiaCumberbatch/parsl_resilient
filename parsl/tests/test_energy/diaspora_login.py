from diaspora_event_sdk import Client as GlobusClient
c = GlobusClient()
# print(c.create_key())
print(c.retrieve_key())
topic = "radio-test"
print(c.register_topic(topic))
print(c.list_topics())

from diaspora_event_sdk import block_until_ready
assert block_until_ready()
