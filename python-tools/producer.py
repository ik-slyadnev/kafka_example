from kafka import KafkaProducer
import json
import time
import logging
import argparse
import sys  # Добавляем импорт sys

# Настройка логирования
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def json_serializer(data):
    return json.dumps(data).encode('utf-8')

def create_producer():
    try:
        producer = KafkaProducer(
            bootstrap_servers=['kafka:9092'],
            value_serializer=json_serializer,
            acks='all',
            retries=3,
            retry_backoff_ms=500,
            # auto_create_topics=False
        )
        logger.info("Kafka Producer успешно создан")
        return producer
    except Exception as e:
        logger.error(f"Ошибка при создании Kafka Producer: {e}")
        raise

def send_message(producer, topic, message, key=None):
    try:
        future = producer.send(topic, value=message, key=key.encode('utf-8') if key else None)
        record_metadata = future.get(timeout=10)
        logger.info(f"Сообщение отправлено в топик {topic}, партиция {record_metadata.partition}, смещение {record_metadata.offset}")
        return record_metadata
    except Exception as e:
        logger.error(f"Ошибка при отправке сообщения: {e}")
        raise

def main():
    parser = argparse.ArgumentParser(description='Kafka Producer Tool')
    parser.add_argument('--topic', required=True, help='Имя топика Kafka')
    parser.add_argument('--key', help='Ключ сообщения (опционально)')
    parser.add_argument('--count', type=int, default=10, help='Количество сообщений')
    parser.add_argument('--interval', type=float, default=1.0, help='Интервал между сообщениями в секундах')
    parser.add_argument('--message', help='Произвольное сообщение в формате JSON (опционально)')
    args = parser.parse_args()

    topic_name = args.topic
    key = args.key
    count = args.count
    interval = args.interval
    user_message = args.message

    producer = create_producer()

    try:
        for i in range(count):
            if user_message:
                # Если передано сообщение, используем его (разбираем JSON)
                try:
                    message = json.loads(user_message)
                except Exception as e:
                    logger.error(f"Ошибка разбора JSON: {e}")
                    sys.exit(1)
            else:
                message = {
                    "id": i,
                    "message": f"Тестовое сообщение {i}",
                    "timestamp": time.time()
                }
            # Если ключ не задан, используем id % 3
            msg_key = key if key is not None else str(i % 3)
            send_message(producer, topic_name, message, msg_key)
            time.sleep(interval)
    finally:
        producer.flush()
        producer.close()
        logger.info("Producer закрыт")

if __name__ == "__main__":
    main()
