from kafka import KafkaConsumer
from json import loads
from time import sleep

# consumer_timeout_ms
# : number of milliseconds to block during message iteration before raising StopIteration (i.e., ending the iterator).
#   Default block forever [float(‘inf’)].

consumer = KafkaConsumer(
   'topic_test',
   bootstrap_servers=['localhost:9092'],
   auto_offset_reset='earliest',
   enable_auto_commit=True,
   group_id='my-group-id',
   value_deserializer=lambda x: loads(x.decode('utf-8')),
   consumer_timeout_ms=3000
)

try:
   for event in consumer:
      event_data = event.value
      # Do whatever you want
      print(event_data)
      sleep(1)
   consumer.close()
except Exception as ex:
   print('Exception in consuming')
   print(str(ex))

