#!/usr/bin/env python3
"""
Консьюмер для чтения сообщений в формате Avro из топика Kafka.
Использование:
    python avro_consumer.py --topic TOPIC_NAME [--group GROUP_ID] [--from-beginning]
"""

import argparse
import json
import signal
import sys
from confluent_kafka import DeserializingConsumer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import StringDeserializer

running = True

def signal_handler(sig, frame):
    """Обработчик сигнала для корректного завершения работы."""
    global running
    print("\nЗавершение работы консьюмера...")
    running = False

def create_avro_consumer(topic, group_id=None, from_beginning=False):
    """Создает и возвращает экземпляр DeserializingConsumer с Avro-десериализатором."""
    schema_registry_client = SchemaRegistryClient({'url': 'http://schema-registry:8081'})
    
    avro_deserializer = AvroDeserializer(
        schema_registry_client,
        lambda x, ctx: x
    )
    
    consumer_config = {
        'bootstrap.servers': 'kafka:9092',
        'key.deserializer': StringDeserializer('utf_8'),
        'value.deserializer': avro_deserializer,
        'group.id': group_id if group_id else 'avro-consumer-group',
        'auto.offset.reset': 'earliest' if from_beginning else 'latest',
        'enable.auto.commit': True
    }
    
    consumer = DeserializingConsumer(consumer_config)
    consumer.subscribe([topic])
    
    return consumer

def print_message(msg):
    """Выводит информацию о сообщении в консоль."""
    print(f"Топик: {msg.topic()}, Партиция: {msg.partition()}, Смещение: {msg.offset()}")
    print(f"Ключ: {msg.key()}")
    print(f"Значение: {json.dumps(msg.value(), ensure_ascii=False)}")
    print(f"Timestamp: {msg.timestamp()[1]}")
    print("-" * 50)

def main():
    parser = argparse.ArgumentParser(description='Kafka Avro Consumer')
    parser.add_argument('--topic', required=True, help='Имя топика Kafka')
    parser.add_argument('--group', help='ID группы консьюмеров (опционально)')
    parser.add_argument('--from-beginning', action='store_true', 
                        help='Читать сообщения с начала топика')
    parser.add_argument('--limit', type=int, help='Максимальное количество сообщений для чтения')
    
    args = parser.parse_args()
    
    signal.signal(signal.SIGINT, signal_handler)
    
    print(f"Запуск Avro консьюмера для топика: {args.topic}")
    if args.group:
        print(f"Группа консьюмеров: {args.group}")
    print(f"Чтение {'с начала топика' if args.from_beginning else 'только новых сообщений'}")
    print("Нажмите Ctrl+C для завершения работы")
    print("-" * 50)
    
    consumer = create_avro_consumer(args.topic, args.group, args.from_beginning)
    
    try:
        message_count = 0
        while running:
            msg = consumer.poll(1.0)
            
            if msg is None:
                continue
            
            if msg.error():
                print(f"Ошибка потребления: {msg.error()}")
                continue
            
            print_message(msg)
            message_count += 1
            
            if args.limit and message_count >= args.limit:
                print(f"Достигнут лимит сообщений ({args.limit}). Завершение работы.")
                break
    finally:
        consumer.close()
        print("Консьюмер закрыт.")

if __name__ == "__main__":
    main()