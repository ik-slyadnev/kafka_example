#!/usr/bin/env python3
from kafka import KafkaConsumer, KafkaProducer
import json
from consumer import deserialize_value
import time
import logging
import threading
import signal
import sys
from argparse import ArgumentParser

# Настройка логирования
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def create_consumer(topic, group_id, client_id=None):
    """Создание потребителя с указанным client_id"""
    return KafkaConsumer(
        topic,
        bootstrap_servers=['kafka:9092'],
        group_id=group_id,
        client_id=client_id,
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        value_deserializer=deserialize_value
    )

def consumer_thread(topic, group_id, client_id, stop_event, errors):
    """Функция для запуска потребителя в отдельном потоке"""
    consumer = None
    try:
        consumer = create_consumer(topic, group_id, client_id)
        logger.info(f"Запущен консьюмер {client_id} в группе {group_id}")
        while not stop_event.is_set():
            messages = consumer.poll(timeout_ms=1000)
            for tp, records in messages.items():
                for record in records:
                    logger.info(f"Консьюмер {client_id}: {tp.topic}-{tp.partition}, "
                                f"смещение={record.offset}, значение={record.value}")
    except Exception as error:
        errors.append(error)
        stop_event.set()
    finally:
        if consumer is not None:
            consumer.close()


def test_consumer_group_rebalancing(topic, group_id, num_consumers=3, duration=30):
    """Тестирование ребалансировки группы потребителей"""
    stop_event = threading.Event()
    consumers = []
    errors = []
    
    try:
        # Запуск потребителей
        for i in range(num_consumers):
            client_id = f"consumer-{i}"
            thread = threading.Thread(
                target=consumer_thread,
                args=(topic, group_id, client_id, stop_event, errors)
            )
            thread.start()
            consumers.append(thread)
            if stop_event.wait(5):
                break
        
            # Отправка тестовых сообщений после запуска каждого потребителя
            producer = KafkaProducer(
                bootstrap_servers=['kafka:9092'],
                value_serializer=lambda x: json.dumps(x).encode('utf-8')
            )
        
            try:
                for j in range(5):
                    message = {
                        "consumer_test_id": i,
                        "message_id": j,
                        "timestamp": time.time()
                    }
                    producer.send(topic, value=message).get(timeout=10)
                producer.flush()

            finally:
                producer.close()
    
        # Ожидание указанного времени
        stop_event.wait(duration)

    finally:
        stop_event.set()
        for thread in consumers:
            thread.join()
    if errors:
        raise RuntimeError("Ошибка потребителя") from errors[0]

def main():
    parser = ArgumentParser(description='Тестирование групп потребителей Kafka')
    parser.add_argument('--topic', required=True, help='Имя топика')
    parser.add_argument('--group', required=True, help='ID группы потребителей')
    parser.add_argument('--consumers', type=int, default=3, help='Количество потребителей')
    parser.add_argument('--duration', type=int, default=30, help='Продолжительность теста в секундах')
    
    args = parser.parse_args()
    if args.consumers <= 0 or args.duration < 0:
        parser.error("--consumers должен быть больше нуля, --duration неотрицательным")
    
    def signal_handler(signum, frame):
        logger.info("Получен сигнал остановки, завершение работы...")
        sys.exit(0)
    
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    test_consumer_group_rebalancing(
        args.topic,
        args.group,
        args.consumers,
        args.duration
    )

if __name__ == "__main__":
    main()