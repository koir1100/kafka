import json

from kafka.consumer import KafkaConsumer


def key_deserializer(key):
    return key.decode('utf-8')


def value_deserializer(value):
    return json.loads(value.decode('utf-8'))


def main():
    topic_name = "fake_people"
    bootstrap_servers = ["localhost:9092"]
    consumer_group_id = "fake_people_group"

    # auto_offset_reset는 해당 topic에 무조건 맨 앞에 있는 메시지를 읽는 것이 아니라
    # 앞서 읽은 기록이 있다면 그 기록 이후부터 읽어오게 됨
    consumer = KafkaConsumer(
        bootstrap_servers=bootstrap_servers,
        group_id=consumer_group_id,
        key_deserializer=key_deserializer,
        value_deserializer=value_deserializer,
        auto_offset_reset='earliest',
        enable_auto_commit=True)

    consumer.subscribe([topic_name])
    for record in consumer:
        print(f"""
            Consumed person {record.value} with key '{record.key}'
            from partition {record.partition} at offset {record.offset}
        """)


if __name__ == '__main__':
    main()
