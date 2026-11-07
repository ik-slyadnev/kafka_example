from kafka import KafkaProducer
import json
import time
import logging
import argparse

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
            max_block_ms=10000,
        )
        logger.info("Kafka Producer успешно создан")
        return producer
    except Exception as e:
        logger.error(f"Ошибка при создании Kafka Producer: {e}")
        raise

def send_message(producer, topic, message, key=None, partition=None):
    try:
        future = producer.send(topic, value=message, key=key.encode('utf-8') if key is not None else None, partition=partition)
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
    parser.add_argument('--count', type=int, default=1, help='Количество сообщений (по умолчанию 1)')
    parser.add_argument('--interval', type=float, default=1.0, help='Интервал между сообщениями в секундах')
    parser.add_argument('--message', help='Сообщение: JSON или обычный текст (опционально)')
    args = parser.parse_args()
    if args.count <= 0 or args.interval < 0:
        parser.error("--count должен быть больше нуля, --interval неотрицательным")

    topic_name = args.topic
    key = args.key
    count = args.count
    interval = args.interval
    user_message = args.message

    producer = create_producer()

    try:
        for i in range(count):
            if user_message is not None:
                try:
                    message = json.loads(user_message)
                except json.JSONDecodeError:
                    message = user_message
            else:
                message = {
                    "id": i,
                    "message": f"Тестовое сообщение {i}",
                    "timestamp": time.time()
                }
            if key is not None:
                # С ключом партицию выбирает Kafka: одинаковый ключ всегда попадает в одну партицию
                send_message(producer, topic_name, message, key)
            else:
                # Без ключа раскладываем сообщения по партициям по кругу: 0, 1, 2, 0, 1, 2...
                partitions = sorted(producer.partitions_for(topic_name) or [])
                if not partitions:
                    raise ValueError(f"Топик {topic_name} не найден")
                send_message(producer, topic_name, message, partition=partitions[i % len(partitions)])
            time.sleep(interval)
    finally:
        producer.flush()
        producer.close()
        logger.info("Producer закрыт")

if __name__ == "__main__":
    main()
