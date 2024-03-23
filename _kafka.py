import json
import ssl
from time import sleep
from collections import namedtuple
from datetime import datetime

from kafka import KafkaProducer, KafkaConsumer, TopicPartition

BOOTSTRAP_SERVERS = ['localhost:9092']
POLL_TIMEOUT_SECONDS = 5
FETCH_TIMEOUT_SECONDS = 1
SESSION_TIMEOUT_SECONDS = 60
HEARTBEAT_INTERVAL_SECONDS = 20
CONSUME_MESSAGE_COUNT = 1000
MAX_PARTITION_FETCH_SIZE_MB = 200
CLIENT_ID = 'etl.airflow'

TopicData = namedtuple('TopicData', 'messages offset')


def send_json_messages_to_queue(
        topic_name: str,
        data: list,
        headers: list = None,
        key: str = None,
        bootstrap_servers: list = BOOTSTRAP_SERVERS,
        use_sasl: bool = False,
        sasl_username: str = None,
        sasl_password: str = None,
        ca_string: str = None
) -> None:
    """
    Загрузка JSON-данных в очередь.

    :param topic_name: Название топика.
    :param data: list(json) Список данных.
    :param headers: Список заголовков, которые нужно передать в метаданных.
    :param key: Ключ к записям.
    :param bootstrap_servers: Сервера брокера.
    :param use_sasl: Использовать подключение по протоколу SASL.
    :param sasl_username: Логин пользователя для SASL.
    :param sasl_password: Пароль пользователя для SASL.
    :param ca_string: Строка файла сертификата.
    :return: None.
    """
    ssl_context = None
    if use_sasl:
        ssl_context = ssl.SSLContext(ssl.PROTOCOL_TLS)
        ssl_context.verify_mode = ssl.CERT_REQUIRED
        ssl_context.load_verify_locations(cadata=ca_string)

    producer = KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        key_serializer=lambda k: k.encode('utf8') if key else None,
        value_serializer=lambda v: json.dumps(v, default=str, ensure_ascii=False).encode('utf8'),
        acks='all',
        retries=3,
        request_timeout_ms=60000,
        ssl_context=ssl_context,
        security_protocol='SASL_SSL' if use_sasl else 'PLAINTEXT',
        sasl_mechanism='SCRAM-SHA-512' if use_sasl else None,
        sasl_plain_username=sasl_username if use_sasl else None,
        sasl_plain_password=sasl_password if use_sasl else None
    )
    for row in data:
        producer.send(
            topic=topic_name,
            headers=headers,
            key=key,
            value=row
        )

    producer.flush()
    producer.close()


def get_topics(group: str) -> list:
    """
    Получение списка топиков.

    :param group: Наименование consumer group.
    :return Список топиков.
    """
    consumer = KafkaConsumer(
        group_id=group,
        bootstrap_servers=BOOTSTRAP_SERVERS
    )
    topics = consumer.topics()
    return sorted(topics)


def fetch_messages_from_topic(
        topic: str,
        consumer_group: str,
        message_count: int = CONSUME_MESSAGE_COUNT,
        offset_reset: str = 'earliest',
        commit_offset: bool = True,
        poll_timeout_seconds: int = POLL_TIMEOUT_SECONDS,
        max_partition_fetch_mb: int = MAX_PARTITION_FETCH_SIZE_MB,
        extract_debezium_payload: bool = False
) -> TopicData:
    """
    Получение сообщений из топика.

    :param topic: Имя топика.
    :param consumer_group: Имя consumer group.
    :param message_count: Кол-во сообщений для получения.
    :param offset_reset: earliest - просмотр очереди с начала, latest - просмотр очереди с конца.
    :param commit_offset: Фиксация offset после получения сообщений.
    :param poll_timeout_seconds: Кол-во секунд до окончания опроса очереди.
    :param max_partition_fetch_mb: Максимальный размер ответа брокера в МБ.
    :param extract_debezium_payload: Извлечение данных из сообщений debezium.
    :return: NamedTuple(messages, offset).
    """
    consumer = KafkaConsumer(
        client_id=CLIENT_ID,
        group_id=consumer_group,
        bootstrap_servers=BOOTSTRAP_SERVERS,
        auto_offset_reset=offset_reset,
        max_partition_fetch_bytes=max_partition_fetch_mb * 1024 * 1024
    )
    partitions = consumer.partitions_for_topic(topic)
    topic_partitions = [TopicPartition(topic, partition) for partition in partitions]
    consumer.assign(topic_partitions)
    messages = consumer.poll(poll_timeout_seconds * 1000, message_count, commit_offset)
    topic_messages = []
    current_offset = None
    for _topic_meta, records in messages.items():
        for message in records:
            timestamp = message.timestamp
            current_offset = message.offset
            value = message.value
            if value is None:
                continue

            json_value = json.loads(value.decode('utf8'))
            topic_messages.append({
                **json_value,
                '__timestamp': timestamp
            })

    if extract_debezium_payload and topic_messages:
        topic_messages = extract_debezium_payload_from_messages(topic_messages)

    sleep(FETCH_TIMEOUT_SECONDS)
    consumer.close()
    return TopicData(topic_messages, current_offset)


def extract_debezium_payload_from_messages(messages: list) -> list:
    """
    Извлечение полезных данных debezium из сообщений с дополнительным преобразованием дат.

    :param messages: Список сообщений.
    :return: Список извлеченных payload из сообщений.
    """
    results = []
    fields_to_fix = get_debezium_fields_to_fix(messages[0])
    for i, record in enumerate(messages):
        try:
            record_payload = record.get('payload')
            after_payload = record_payload.get('after')
            before_payload = record_payload.get('before')
            op_timestamp = record_payload.get('ts_ms')
            result_payload = None
            if after_payload is not None:
                result_payload = fix_debezium_payload(
                    {**after_payload, '__op_ts': op_timestamp},
                    fields_to_fix
                )
            elif before_payload is not None:
                result_payload = fix_debezium_payload(
                    {
                        **before_payload,
                        '__op_ts': op_timestamp,
                        '__record_removed': True
                    },
                    fields_to_fix
                )

            if result_payload is not None:
                results.append(result_payload)
        except Exception as e:
            message = f"Error on processing message.\nstr({e})\n{record}"
            raise Exception(message)

    return results


def get_debezium_fields_to_fix(message: dict) -> list:
    """
    Получение списка полей для исправления в них данных.

    :param message: Сообщение.
    :return: Список полей для исправления.
    """
    schema = message.get('schema')
    fields_to_fix = []
    for field_meta in schema.get('fields'):
        if field_meta.get('field') == 'after':
            for field in field_meta.get('fields'):
                if 'name' in field:
                    field_type = field.get('name').split('.')[-1]
                    filter_types = (
                        'MicroTimestamp',
                    )
                    if any((field_type.endswith(filter_type) for filter_type in filter_types)):
                        fields_to_fix.append({
                            'name': field.get('field'),
                            'type': field_type,
                        })

    return fields_to_fix


def fix_debezium_payload(payload: dict, fields: list) -> dict:
    """
    Корректировка полей некоторых типов.

    :param payload: Данные сообщения.
    :param fields: Поля для корректировки.
    :return: Скорректированное сообщение.
    """
    fixed_values = {}
    for field in fields:
        field_type = field.get('type')
        field_name = field.get('name')
        value = payload.get(field_name)
        if value is not None:
            if field_type == 'MicroTimestamp':
                correct_value = datetime.fromtimestamp(float(value) / 1_000_000)
                correct_value = correct_value.strftime('%Y-%m-%d %H:%M:%S.%f')
                fixed_values[field_name] = correct_value

    return {**payload, **fixed_values}
