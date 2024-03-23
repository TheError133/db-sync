from db import get_source_data
from _kafka import send_json_messages_to_queue


if __name__ == '__main__':
    source_data = get_source_data()
    topic = 'test'
    send_json_messages_to_queue(topic, source_data)

    print()
