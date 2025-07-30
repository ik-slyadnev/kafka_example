#!/usr/bin/env python3
"""
Утилита для управления топиками Kafka.
Использование:
    python topic_manager.py --create TOPIC_NAME [--partitions N] [--replication N]
    python topic_manager.py --list
    python topic_manager.py --describe TOPIC_NAME
    python topic_manager.py --delete TOPIC_NAME
"""

import argparse
import json
from kafka import KafkaAdminClient
from kafka.admin import NewTopic
from kafka.errors import TopicAlreadyExistsError, UnknownTopicOrPartitionError

def create_admin_client():
    """Создает и возвращает экземпляр KafkaAdminClient."""
    return KafkaAdminClient(
        bootstrap_servers=['kafka:9092']
    )

def create_topic(admin_client, topic_name, num_partitions=1, replication_factor=1):
    """Создает новый топик в Kafka."""
    try:
        topic = NewTopic(
            name=topic_name,
            num_partitions=num_partitions,
            replication_factor=replication_factor
        )
        admin_client.create_topics([topic])
        print(f"Топик '{topic_name}' успешно создан с {num_partitions} партициями и фактором репликации {replication_factor}")
    except TopicAlreadyExistsError:
        print(f"Топик '{topic_name}' уже существует")

def list_topics(admin_client):
    """Выводит список всех топиков в Kafka."""
    topics = admin_client.list_topics()
    print("Доступные топики:")
    for topic in sorted(topics):
        print(f"- {topic}")
    return topics

def describe_topic(admin_client, topic_name):
    """Выводит подробную информацию о топике."""
    try:
        topic_metadata = admin_client.describe_topics([topic_name])
        print(f"Информация о топике '{topic_name}':")
        print(json.dumps(topic_metadata, indent=2, default=str))
        
        # Получаем конфигурацию топика
        configs = admin_client.describe_configs(
            config_resources=[
                (2, topic_name)  # 2 - это RESOURCE_TYPE_TOPIC
            ]
        )
        print("\nКонфигурация топика:")
        for resource, config_entries in configs.items():
            for config_name, config_value in config_entries.items():
                print(f"  {config_name}: {config_value}")
    except UnknownTopicOrPartitionError:
        print(f"Топик '{topic_name}' не существует")

def delete_topic(admin_client, topic_name):
    """Удаляет топик из Kafka."""
    try:
        admin_client.delete_topics([topic_name])
        print(f"Топик '{topic_name}' успешно удален")
    except UnknownTopicOrPartitionError:
        print(f"Топик '{topic_name}' не существует")

def main():
    parser = argparse.ArgumentParser(description='Kafka Topic Manager')
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument('--create', metavar='TOPIC_NAME', help='Создать новый топик')
    group.add_argument('--list', action='store_true', help='Показать список всех топиков')
    group.add_argument('--describe', metavar='TOPIC_NAME', help='Показать информацию о топике')
    group.add_argument('--delete', metavar='TOPIC_NAME', help='Удалить топик')
    
    parser.add_argument('--partitions', type=int, default=1, help='Количество партиций (для --create)')
    parser.add_argument('--replication', type=int, default=1, help='Фактор репликации (для --create)')
    
    args = parser.parse_args()
    
    admin_client = create_admin_client()
    
    try:
        if args.create:
            create_topic(admin_client, args.create, args.partitions, args.replication)
        elif args.list:
            list_topics(admin_client)
        elif args.describe:
            describe_topic(admin_client, args.describe)
        elif args.delete:
            delete_topic(admin_client, args.delete)
    finally:
        admin_client.close()

if __name__ == "__main__":
    main()