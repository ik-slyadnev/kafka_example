#!/usr/bin/env python3
from kafka import KafkaProducer, KafkaConsumer, errors
import json
import time
import logging
from argparse import ArgumentParser
import signal
import sys

# Настройка логирования
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def test_connection_error():
    """Тестирование ошибки подключения"""
    try:
        producer = KafkaProducer(
            bootstrap_servers=['non-existent-host:9092'],
            value_serializer=lambda x: json.dumps(x).encode('utf-8')
        )
    except errors.KafkaError as e:
        logger.error(f"Ожидаемая ошибка подключения: {e}")
        return True
    return False

def test_topic_not_exists(topic):
    """Тестирование отправки в несуществующий топик"""
    producer = KafkaProducer(
        bootstrap_servers=['kafka:9092'],
        value_serializer=lambda x: json.dumps(x).encode('utf-8')
    )
    
    try:
        future = producer.send(topic, value={"test": "message"})
        future.get(timeout=10)
    except errors.KafkaError as e:
        logger.error(f"Ожидаемая ошибка топика: {e}")
        return True
    finally:
        producer.close()
    return False

def test_serialization_error(topic):
    """Тестирование ошибки сериализации"""
    producer = KafkaProducer(
        bootstrap_servers=['kafka:9092'],
        value_serializer=lambda x: json.dumps(x).encode('utf-8')
    )
    
    try:
        # Создаем объект, который нельзя сериализовать в JSON
        class UnserializableObject:
            pass
        
        future = producer.send(topic, value=UnserializableObject())
        future.get(timeout=10)
    except TypeError as e:
        logger.error(f"Ожидаемая ошибка сериализации: {e}")
        return True
    finally:
        producer.close()
    return False

def test_consumer_timeout():
    """Тестирование таймаута потребителя"""
    consumer = KafkaConsumer(
        'non-existent-topic',
        bootstrap_servers=['kafka:9092'],
        consumer_timeout_ms=1000  # Таймаут 1 секунда
    )
    
    try:
        next(consumer)
    except StopIteration:
        logger.error("Ожидаемый таймаут потребителя")
        return True
    finally:
        consumer.close()
    return False

def main():
    parser = ArgumentParser(description='Тестирование сценариев ошибок Kafka')
    parser.add_argument('--scenario', choices=['connection', 'topic', 'serialization', 'timeout'],
                       required=True, help='Сценарий тестирования')
    parser.add_argument('--topic', help='Имя топика для тестирования')
    
    args = parser.parse_args()
    
    def signal_handler(signum, frame):
        logger.info("Получен сигнал остановки, завершение работы...")
        sys.exit(0)
    
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    scenarios = {
        'connection': test_connection_error,
        'topic': lambda: test_topic_not_exists(args.topic),
        'serialization': lambda: test_serialization_error(args.topic),
        'timeout': test_consumer_timeout
    }
    
    if args.scenario in scenarios:
        success = scenarios[args.scenario]()
        logger.info(f"Тест сценария {args.scenario}: {'успешно' if success else 'неуспешно'}")
    else:
        logger.error(f"Неизвестный сценарий: {args.scenario}")

if __name__ == "__main__":
    main()