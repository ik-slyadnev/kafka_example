#!/usr/bin/env python3
"""
Простой консьюмер Kafka для чтения сообщений из топика.
Использование:
    python consumer.py --topic TOPIC_NAME [--group GROUP_ID] [--from-beginning] [--limit LIMIT]
"""

import argparse
import json
import signal
import sys
from kafka import KafkaConsumer

running = True

def signal_handler(sig, frame):
    """Обработчик сигнала для корректного завершения работы."""
    global running
    print("\nЗавершение работы консьюмера...")
    running = False

def deserialize_value(value):
    if value is None:
        return None
    text = value.decode('utf-8')
    try:
        return json.loads(text)
    except json.JSONDecodeError:
        return text


def create_consumer(topic, group_id=None, from_beginning=False):
    """Создает и возвращает экземпляр KafkaConsumer."""
    auto_offset_reset = 'earliest' if from_beginning else 'latest'
    
    consumer = KafkaConsumer(
        topic,
        bootstrap_servers=['kafka:9092'],
        auto_offset_reset=auto_offset_reset,
        enable_auto_commit=True,
        group_id=group_id,
        consumer_timeout_ms=1000,  # раз в секунду выходим из ожидания, чтобы Ctrl+C срабатывал сразу
        value_deserializer=deserialize_value,
        key_deserializer=lambda x: x.decode('utf-8') if x else None
    )
    
    return consumer

def print_message(msg):
    """Выводит информацию о сообщении в консоль."""
    print(f"Топик: {msg.topic}, Партиция: {msg.partition}, Смещение: {msg.offset}")
    print(f"Ключ: {msg.key}")
    print(f"Значение: {json.dumps(msg.value, ensure_ascii=False)}")
    print(f"Timestamp: {msg.timestamp}")
    print("-" * 50)

def main():
    parser = argparse.ArgumentParser(description='Kafka Consumer Tool')
    parser.add_argument('--topic', required=True, help='Имя топика Kafka')
    parser.add_argument('--group', help='ID группы консьюмеров (опционально)')
    parser.add_argument('--from-beginning', action='store_true', 
                        help='Читать сообщения с начала топика')
    parser.add_argument('--limit', type=int, help='Максимальное количество сообщений для чтения')
    
    args = parser.parse_args()
    if args.limit is not None and args.limit <= 0:
        parser.error("--limit должен быть больше нуля")
    
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    print(f"Запуск консьюмера для топика: {args.topic}")
    if args.group:
        print(f"Группа консьюмеров: {args.group}")
    print(f"Чтение {'с начала топика' if args.from_beginning else 'только новых сообщений'}")
    print("Нажмите Ctrl+C для завершения работы")
    print("-" * 50)
    
    consumer = create_consumer(args.topic, args.group, args.from_beginning)
    
    try:
        message_count = 0
        limit_reached = False
        while running and not limit_reached:
            for message in consumer:
                print_message(message)
                message_count += 1

                if args.limit and message_count >= args.limit:
                    print(f"Достигнут лимит сообщений ({args.limit}). Завершение работы.")
                    limit_reached = True
                    break
                if not running:
                    break
    finally:
        consumer.close()
        print("Консьюмер закрыт.")

if __name__ == "__main__":
    main()