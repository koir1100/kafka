import json

from kafka import TopicPartition, OffsetAndMetadata
from kafka.consumer import KafkaConsumer


def key_deserializer(key):
    return key.decode('utf-8')


def value_deserializer(value):
    return json.loads(value.decode('utf-8'))


def main():
    topic_name = "fake_people"
    bootstrap_servers = ["localhost:9092"]
    consumer_group_id = "manual_fake_people_group"

    consumer = KafkaConsumer(
        bootstrap_servers=bootstrap_servers,
        group_id=consumer_group_id,
        key_deserializer=key_deserializer,
        value_deserializer=value_deserializer,
        auto_offset_reset='earliest',
        enable_auto_commit=False)

    consumer.subscribe([topic_name])
    for record in consumer:
        print(f"""
            Consumed person {record.value} with key '{record.key}'
            from partition {record.partition} at offset {record.offset}
        """)

        topic_partition = TopicPartition(record.topic, record.partition)
        offset = OffsetAndMetadata(record.offset + 1, record.timestamp)

        # 읽을 때마다 하나씩 commit을 하면 정합성 측면에서는 의미가 있으나 리소스가 필요
        # (백그라운드 형태의) autocommit과 유사하게 batch 형태로 commit을 하여 읽은 상황 기록을 업데이트 수행
        consumer.commit({
            topic_partition: offset
        })

if __name__ == '__main__':
    main()
