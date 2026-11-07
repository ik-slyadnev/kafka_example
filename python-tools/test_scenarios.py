#!/usr/bin/env python3
"""
Набор тестовых сценариев для отработки навыков работы с Kafka.
Использование:
    python test_scenarios.py --scenario SCENARIO_NAME
"""

import argparse
import json
import random
import subprocess
import shlex
import tempfile
import time

# Список доступных сценариев
SCENARIOS = [
    "basic_producer_consumer",
    "multiple_partitions",
    "consumer_groups",
    "avro_schema",
    "error_handling",
    "rebalancing"
]

def run_command(command):
    """Ошибка дочерней команды прерывает сценарий, ожидание ограничено."""
    print(f"Выполнение команды: {command}", flush=True)
    return subprocess.run(shlex.split(command), check=True, timeout=120)


def basic_producer_consumer():
    """
    Базовый сценарий: создание топика, отправка сообщений и их чтение.
    """
    print("=== Сценарий: Базовый продюсер и консьюмер ===")
    
    # Создаем топик
    topic_name = f"test-topic-{random.randint(1000, 9999)}"
    run_command(f"python topic_manager.py --create {topic_name} --partitions 1")
    
    # Отправляем сообщения
    print("\nОтправка сообщений в топик...")
    run_command(f"python producer.py --topic {topic_name} --count 5 --interval 1")
    
    # Читаем сообщения
    print("\nЧтение сообщений из топика...")
    run_command(f"python consumer.py --topic {topic_name} --from-beginning --limit 5")
    
    print("\nСценарий завершен. Топик можно удалить командой:")
    print(f"python topic_manager.py --delete {topic_name}")

def multiple_partitions():
    """
    Сценарий с несколькими партициями: демонстрация распределения сообщений по партициям.
    """
    print("=== Сценарий: Работа с несколькими партициями ===")
    
    # Создаем топик с несколькими партициями
    topic_name = f"multi-part-topic-{random.randint(1000, 9999)}"
    run_command(f"python topic_manager.py --create {topic_name} --partitions 3")
    
    # Отправляем сообщения с разными ключами
    print("\nОтправка сообщений с разными ключами...")
    for i in range(3):
        key = f"key{i}"
        run_command(f"python producer.py --topic {topic_name} --key {key} --count 3 --interval 0.5")
    
    # Читаем сообщения
    print("\nЧтение сообщений из топика...")
    run_command(f"python consumer.py --topic {topic_name} --from-beginning --limit 9")
    
    print("\nСценарий завершен. Топик можно удалить командой:")
    print(f"python topic_manager.py --delete {topic_name}")

def consumer_groups():
    """
    Сценарий с группами потребителей: демонстрация работы нескольких консьюмеров в группе.
    """
    print("=== Сценарий: Работа с группами потребителей ===")
    
    # Создаем топик с несколькими партициями
    topic_name = f"group-topic-{random.randint(1000, 9999)}"
    run_command(f"python topic_manager.py --create {topic_name} --partitions 3")
    
    # Запускаем несколько консьюмеров в одной группе
    print("\nЗапуск нескольких консьюмеров в одной группе...")
    print("Откройте 3 разных терминала и выполните в них следующие команды:")
    group_id = f"test-group-{random.randint(1000, 9999)}"
    
    print(f"\nТерминал 1: docker exec -it python-tools python consumer.py --topic {topic_name} --group {group_id} --from-beginning")
    print(f"Терминал 2: docker exec -it python-tools python consumer.py --topic {topic_name} --group {group_id} --from-beginning")
    print(f"Терминал 3: docker exec -it python-tools python consumer.py --topic {topic_name} --group {group_id} --from-beginning")
    
    print("\nПосле запуска всех трёх консьюмеров отправьте сообщения в четвёртом терминале:")
    print(f"docker exec -it python-tools python producer.py --topic {topic_name} --count 10 --interval 0.5")
    print("\nОбратите внимание, как сообщения распределяются между консьюмерами в группе.")
    print("После тестирования нажмите Ctrl+C в каждом терминале для остановки консьюмеров.")
    
    print("\nСценарий завершен. Топик можно удалить командой:")
    print(f"python topic_manager.py --delete {topic_name}")

def avro_schema():
    """
    Сценарий с использованием Avro и Schema Registry.
    """
    print("=== Сценарий: Работа с Avro и Schema Registry ===")
    
    # Создаем топик
    topic_name = f"avro-topic-{random.randint(1000, 9999)}"
    run_command(f"python topic_manager.py --create {topic_name} --partitions 1")
    
    # Отправляем сообщения с использованием схемы Avro
    print("\nОтправка сообщений с использованием схемы Avro...")
    run_command(f"python avro_producer.py --topic {topic_name} --schema schemas/user_event.avsc --data data/sample_events.json")
    
    # Читаем сообщения с использованием Avro-консьюмера
    print("\nЧтение сообщений с использованием Avro-консьюмера...")
    run_command(f"python avro_consumer.py --topic {topic_name} --from-beginning --limit 4")
    
    print("\nСценарий завершен. Топик можно удалить командой:")
    print(f"python topic_manager.py --delete {topic_name}")

def error_handling():
    """
    Сценарий с обработкой ошибок: демонстрация различных ошибок и их обработки.
    """
    print("=== Сценарий: Обработка ошибок ===")
    
    topic = f"serialization-{random.randint(1000, 9999)}"
    run_command(f"python topic_manager.py --create {topic} --partitions 1")
    try:
        for scenario in ('connection', 'topic', 'serialization', 'timeout'):
            selected_topic = topic if scenario == 'serialization' else f"missing-{random.randint(1000, 9999)}"
            run_command(f"python error_scenarios.py --scenario {scenario} --topic {selected_topic}")
    finally:
        run_command(f"python topic_manager.py --delete {topic}")

    with tempfile.NamedTemporaryFile(mode='w', suffix='.avsc') as schema:
        schema.write('{"type": "record", "name": "Invalid", "fields": [{"name": "field1"}]}')
        schema.flush()
        result = subprocess.run(['python', 'avro_producer.py', '--topic', 'invalid-schema-topic',
                                 '--schema', schema.name], timeout=30)
        if result.returncode == 0:
            raise RuntimeError("Неверная схема неожиданно принята")
    
    print("\nСценарий завершен.")

def rebalancing():
    """
    Сценарий с ребалансировкой: демонстрация процесса ребалансировки при добавлении/удалении консьюмеров.
    """
    print("=== Сценарий: Ребалансировка консьюмеров ===")
    
    # Создаем топик с несколькими партициями
    topic_name = f"rebalance-topic-{random.randint(1000, 9999)}"
    run_command(f"python topic_manager.py --create {topic_name} --partitions 4")
    
    # Инструкции по запуску консьюмеров для наблюдения ребалансировки
    print("\nДля наблюдения процесса ребалансировки выполните следующие шаги:")
    group_id = f"rebalance-group-{random.randint(1000, 9999)}"
    
    print(f"\n1. Откройте первый терминал и запустите консьюмер:")
    print(f"   docker exec -it python-tools python consumer.py --topic {topic_name} --group {group_id} --from-beginning")
    print("В отдельном терминале отправьте новую порцию сообщений:")
    print(f"docker exec -it python-tools python producer.py --topic {topic_name} --count 20 --interval 0.2")
    
    print(f"\n2. Через 5-10 секунд откройте второй терминал и запустите еще один консьюмер в той же группе:")
    print(f"   docker exec -it python-tools python consumer.py --topic {topic_name} --group {group_id} --from-beginning")
    print("В отдельном терминале отправьте новую порцию сообщений:")
    print(f"docker exec -it python-tools python producer.py --topic {topic_name} --count 20 --interval 0.2")
    
    print(f"\n3. Через 5-10 секунд откройте третий терминал и запустите третий консьюмер:")
    print(f"   docker exec -it python-tools python consumer.py --topic {topic_name} --group {group_id} --from-beginning")
    print("В отдельном терминале отправьте новую порцию сообщений:")
    print(f"docker exec -it python-tools python producer.py --topic {topic_name} --count 20 --interval 0.2")
    
    print(f"\n4. Наблюдайте за распределением партиций между консьюмерами.")
    
    print(f"\n5. Остановите один из консьюмеров (Ctrl+C) и наблюдайте за ребалансировкой.")
    
    print("\nСценарий завершен. Топик можно удалить командой:")
    print(f"python topic_manager.py --delete {topic_name}")

def main():
    parser = argparse.ArgumentParser(description='Kafka Test Scenarios')
    parser.add_argument('--scenario', required=True, choices=SCENARIOS, help='Имя сценария для запуска')
    
    args = parser.parse_args()
    
    # Запуск выбранного сценария
    if args.scenario == "basic_producer_consumer":
        basic_producer_consumer()
    elif args.scenario == "multiple_partitions":
        multiple_partitions()
    elif args.scenario == "consumer_groups":
        consumer_groups()
    elif args.scenario == "avro_schema":
        avro_schema()
    elif args.scenario == "error_handling":
        error_handling()
    elif args.scenario == "rebalancing":
        rebalancing()

if __name__ == "__main__":
    main()