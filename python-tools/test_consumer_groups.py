#!/usr/bin/env python3
from kafka import KafkaConsumer, KafkaProducer
import json
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
        value_deserializer=lambda x: json.loads(x.decode('utf-8'))
    )

def consumer_thread(topic, group_id, client_id, stop_event):
    """Функция для запуска потребителя в отдельном потоке"""
    consumer = create_consumer(topic, group_id, client_id)
    
    logger.info(f"Запущен консьюмер {client_id} в группе {group_id}")
    
    try:
        while not stop_event.is_set():
            messages = consumer.poll(timeout_ms=1000)
            for tp, records in messages.items():
                for record in records:
                    logger.info(f"Консьюмер {client_id} получил сообщение: "
                              f"топик={tp.topic}, партиция={tp.partition}, "
                              f"смещение={record.offset}, значение={record.value}")
    finally:
        consumer.close()
        logger.info(f"Консьюмер {client_id} остановлен")

def test_consumer_group_rebalancing(topic, group_id, num_consumers=3, duration=30):
    """Тестирование ребалансировки группы потребителей"""
    stop_event = threading.Event()
    consumers = []
    
    # Запуск потребителей
    for i in range(num_consumers):
        client_id = f"consumer-{i}"
        thread = threading.Thread(
            target=consumer_thread,
            args=(topic, group_id, client_id, stop_event)
        )
        thread.start()
        consumers.append(thread)
        time.sleep(5)  # Пауза между запуском потребителей
        
        # Отправка тестовых сообщений после запуска каждого потребителя
        producer = KafkaProducer(
            bootstrap_servers=['kafka:9092'],
            value_serializer=lambda x: json.dumps(x).encode('utf-8')
        )
        
        for j in range(5):
            message = {
                "consumer_test_id": i,
                "message_id": j,
                "timestamp": time.time()
            }
            producer.send(topic, value=message)
        producer.flush()
        producer.close()

    # Ожидание указанного времени
    time.sleep(duration)
    
    # Остановка тестирования
    stop_event.set()
    for thread in consumers:
        thread.join()

def main():
    parser = ArgumentParser(description='Тестирование групп потребителей Kafka')
    parser.add_argument('--topic', required=True, help='Имя топика')
    parser.add_argument('--group', required=True, help='ID группы потребителей')
    parser.add_argument('--consumers', type=int, default=3, help='Количество потребителей')
    parser.add_argument('--duration', type=int, default=30, help='Продолжительность теста в секундах')
    
    args = parser.parse_args()
    
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