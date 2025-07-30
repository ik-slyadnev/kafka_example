#!/usr/bin/env python3
"""
Утилита для работы с Avro и Schema Registry.
Использование:
    python avro_producer.py --topic TOPIC_NAME --schema SCHEMA_FILE [--data DATA_FILE]
"""

import argparse
import json
import random
import time
from datetime import datetime
from confluent_kafka import SerializingProducer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import StringSerializer

def load_schema(schema_file):
    """Загружает схему Avro из файла."""
    with open(schema_file, 'r') as f:
        return f.read()

def load_data(data_file):
    """Загружает данные из JSON-файла."""
    with open(data_file, 'r') as f:
        return json.load(f)

def generate_random_data(schema_json):
    """Генерирует случайные данные на основе схемы Avro."""
    schema = json.loads(schema_json)
    data = {}
    
    for field in schema.get('fields', []):
        field_name = field['name']
        field_type = field['type']
        
        if isinstance(field_type, list):
            # Обрабатываем union типы (например, ["null", "string"])
            non_null_types = [t for t in field_type if t != "null"]
            if not non_null_types:
                data[field_name] = None
                continue
            field_type = random.choice(non_null_types)
        
        if field_type == "string":
            data[field_name] = f"value_{random.randint(1, 100)}"
        elif field_type == "int":
            data[field_name] = random.randint(1, 1000)
        elif field_type == "long":
            data[field_name] = random.randint(1, 10000)
        elif field_type == "float" or field_type == "double":
            data[field_name] = round(random.random() * 100, 2)
        elif field_type == "boolean":
            data[field_name] = random.choice([True, False])
        else:
            data[field_name] = None
    
    return data

def delivery_report(err, msg):
    """Обратный вызов для отчета о доставке сообщений."""
    if err is not None:
        print(f"Ошибка доставки сообщения: {err}")
    else:
        print(f"Сообщение доставлено в {msg.topic()} [{msg.partition()}] со смещением {msg.offset()}")

def create_avro_producer(schema_str):
    """Создает и возвращает экземпляр SerializingProducer с Avro-сериализатором."""
    schema_registry_client = SchemaRegistryClient({'url': 'http://schema-registry:8081'})
    
    avro_serializer = AvroSerializer(
        schema_registry_client,
        schema_str,
        lambda x, ctx: x
    )
    
    producer_config = {
        'bootstrap.servers': 'kafka:9092',
        'key.serializer': StringSerializer('utf_8'),
        'value.serializer': avro_serializer
    }
    
    return SerializingProducer(producer_config)

def main():
    parser = argparse.ArgumentParser(description='Kafka Avro Producer')
    parser.add_argument('--topic', required=True, help='Имя топика Kafka')
    parser.add_argument('--schema', required=True, help='Путь к файлу схемы Avro')
    parser.add_argument('--data', help='Путь к файлу с данными в формате JSON')
    parser.add_argument('--count', type=int, default=1, help='Количество сообщений для отправки')
    parser.add_argument('--interval', type=float, default=1.0, help='Интервал между сообщениями в секундах')
    
    args = parser.parse_args()
    
    schema_str = load_schema(args.schema)
    producer = create_avro_producer(schema_str)
    
    try:
        if args.data:
            data = load_data(args.data)
            if isinstance(data, list):
                for i, item in enumerate(data):
                    key = str(i)
                    producer.produce(
                        topic=args.topic,
                        key=key,
                        value=item,
                        on_delivery=delivery_report
                    )
                    producer.flush()
                    print(f"Отправлено сообщение с ключом {key}: {json.dumps(item, ensure_ascii=False)}")
                    if i < len(data) - 1:  # Не ждем после последнего сообщения
                        time.sleep(args.interval)
            else:
                producer.produce(
                    topic=args.topic,
                    key="0",
                    value=data,
                    on_delivery=delivery_report
                )
                producer.flush()
                print(f"Отправлено сообщение: {json.dumps(data, ensure_ascii=False)}")
        else:
            for i in range(args.count):
                data = generate_random_data(schema_str)
                key = str(i)
                producer.produce(
                    topic=args.topic,
                    key=key,
                    value=data,
                    on_delivery=delivery_report
                )
                producer.flush()
                print(f"Отправлено сообщение с ключом {key}: {json.dumps(data, ensure_ascii=False)}")
                if i < args.count - 1:  # Не ждем после последнего сообщения
                    time.sleep(args.interval)
    finally:
        producer.flush()

if __name__ == "__main__":
    main()