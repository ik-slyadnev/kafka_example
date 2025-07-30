#!/usr/bin/env python3
from kafka import KafkaConsumer, KafkaProducer, TopicPartition
import json
import time
import logging
from argparse import ArgumentParser
import signal
import sys

# Настройка логирования
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def create_producer():
    """Создание производителя"""
    return KafkaProducer(
        bootstrap_servers=['kafka:9092'],
        value_serializer=lambda x: json.dumps(x).encode('utf-8')
    )

def create_consumer(topic, group_id=None, offset='earliest'):
    """Создание потребителя с указанной стратегией смещения"""
    return KafkaConsumer(
        topic,
        bootstrap_servers=['kafka:9092'],
        group_id=group_id,
        auto_offset_reset=offset,
        enable_auto_commit=True,
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )

def send_test_messages(topic, count=10):
    """Отправка тестовых сообщений"""
    producer = create_producer()
    messages_sent = []
    
    for i in range(count):
        message = {
            "message_id": i,
            "content": f"Test message {i}",
            "timestamp": time.time()
        }
        future = producer.send(topic, value=message)
        record_metadata = future.get(timeout=10)
        messages_sent.append({
            "message": message,
            "partition": record_metadata.partition,
            "offset": record_metadata.offset
        })
        logger.info(f"Отправлено сообщение {i} в партицию {record_metadata.partition}, смещение {record_metadata.offset}")
    
    producer.flush()
    producer.close()
    return messages_sent

def test_offset_recovery(topic, group_id, specific_offset=None):
    """Тестирование восстановления с определенного смещения"""
    consumer = create_consumer(topic, group_id)
    
    if specific_offset is not None:
        # Получение списка партиций топика
        partitions = consumer.partitions_for_topic(topic)
        if partitions:
            # Установка указанного смещения для всех партиций
            for partition in partitions:
                tp = TopicPartition(topic, partition)
                consumer.assign([tp])
                consumer.seek(tp, specific_offset)
    
    logger.info(f"Начало чтения с{'указанного смещения ' + str(specific_offset) if specific_offset else ' начала'}")
    
    try:
        for message in consumer:
            logger.info(f"Получено сообщение: топик={message.topic}, "
                       f"партиция={message.partition}, смещение={message.offset}, "
                       f"значение={message.value}")
    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()

def main():
    parser = ArgumentParser(description='Тестирование восстановления данных Kafka')
    parser.add_argument('--topic', required=True, help='Имя топика')
    parser.add_argument('--group', required=True, help='ID группы потребителей')
    parser.add_argument('--mode', choices=['send', 'recover'], required=True,
                       help='Режим работы: send - отправка сообщений, recover - восстановление')
    parser.add_argument('--messages', type=int, default=10,
                       help='Количество сообщений для отправки')
    parser.add_argument('--offset', type=int, help='Конкретное смещение для восстановления')
    
    args = parser.parse_args()
    
    def signal_handler(signum, frame):
        logger.info("Получен сигнал остановки, завершение работы...")
        sys.exit(0)
    
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    if args.mode == 'send':
        send_test_messages(args.topic, args.messages)
    else:
        test_offset_recovery(args.topic, args.group, args.offset)

if __name__ == "__main__":
    main()