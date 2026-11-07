# Apache Kafka для QA: тренажёр с Python и Docker Compose

Локальный тренажёр Apache Kafka для ручного тестирования: топики, партиции, группы потребителей, Avro и Schema Registry, ребалансировка и восстановление после перезапуска брокера
Стенд запускается через Docker Compose; сообщения и состояние брокера можно смотреть в Kafka UI или через Python CLI

Local Apache Kafka training lab for manual QA: topics, partitions, consumer groups, Avro, Schema Registry and recovery scenarios. Includes Kafka UI, Python CLI tools and integration tests.

[Быстрый запуск](#установка-и-запуск) · [Сценарии](#тестовые-сценарии) · [Практика](#задания-для-практики) · [Проверки](#проверка-исправлений)

## Содержание

- [Требования](#требования)
- [Установка и запуск](#установка-и-запуск)
- [Компоненты тренажера](#компоненты-тренажера)
- [Основные команды](#основные-команды)
- [Тестовые сценарии](#тестовые-сценарии)
- [Полезные ресурсы](#полезные-ресурсы)

## Требования

Для работы с тренажером вам понадобится:

- Git
- Docker и Docker Compose с поддержкой `docker compose up --wait`
- Python 3 на компьютере для проверки перезапуска брокера; остальные Python-скрипты работают в контейнере
- Базовые знания работы с терминалом
- Базовое понимание концепций Kafka

## Установка и запуск

1. Клонируйте репозиторий:
   ```
   git clone https://github.com/ik-slyadnev/kafka_example.git
   cd kafka_example
   ```

2. Запустите контейнеры с помощью Docker Compose:
   ```
   docker compose up -d --build --wait
   ```

3. Проверьте, что все контейнеры запущены:
   ```
   docker compose ps
   ```

4. Откройте Kafka UI в браузере:
   ```
   http://localhost:8080
   ```

## Компоненты тренажера

Тренажер включает следующие компоненты:

- **Apache Kafka** - распределенная система обмена сообщениями
- **Zookeeper** - координационный сервис для Kafka
- **Schema Registry** - реестр схем для работы с Avro
- **Kafka UI** - веб-интерфейс для управления Kafka
- **Python Tools** - набор скриптов для работы с Kafka

## Основные команды

### Управление топиками

Создание топика:
```
docker exec -it python-tools python topic_manager.py --create my-topic --partitions 3 --replication 1
```

Просмотр списка топиков:
```
docker exec -it python-tools python topic_manager.py --list
```

Просмотр информации о топике:
```
docker exec -it python-tools python topic_manager.py --describe my-topic
```

Удаление топика после завершения отправки и чтения сообщений:
```
docker exec -it python-tools python topic_manager.py --delete my-topic
```

### Отправка и чтение сообщений

Отправка сообщений:
```
docker exec -it python-tools python producer.py --topic my-topic --message "Тестовое сообщение" --count 5 --interval 1
```

Чтение сообщений:
```
docker exec -it python-tools python consumer.py --topic my-topic --from-beginning --limit 5
```

### Работа с Avro и Schema Registry

Отправка сообщений с использованием Avro:
```
docker exec -it python-tools python topic_manager.py --create avro-topic
docker exec -it python-tools python avro_producer.py --topic avro-topic --schema schemas/user_event.avsc --data data/sample_events.json
```

Чтение сообщений в формате Avro:
```
docker exec -it python-tools python avro_consumer.py --topic avro-topic --group avro-example-first --from-beginning --limit 4
```

## Тестовые сценарии

Тренажер включает набор готовых тестовых сценариев для отработки различных аспектов работы с Kafka:

1. **Базовый продюсер и консьюмер**:
   ```
   docker exec -it python-tools python test_scenarios.py --scenario basic_producer_consumer
   ```

2. **Работа с несколькими партициями**:
   ```
   docker exec -it python-tools python test_scenarios.py --scenario multiple_partitions
   ```

3. **Работа с группами потребителей**:
   ```
   docker exec -it python-tools python test_scenarios.py --scenario consumer_groups
   ```

4. **Работа с Avro и Schema Registry**:
   ```
   docker exec -it python-tools python test_scenarios.py --scenario avro_schema
   ```

5. **Обработка ошибок**:
   ```
   docker exec -it python-tools python test_scenarios.py --scenario error_handling
   ```

6. **Ребалансировка консьюмеров**:
   ```
   docker exec -it python-tools python test_scenarios.py --scenario rebalancing
   ```

## Задания для практики

1. **Базовая работа с топиками**:
   - Создайте новый топик с 3 партициями
   - Отправьте 10 сообщений в этот топик
   - Прочитайте все сообщения из топика
   - Проверьте распределение сообщений по партициям

2. **Работа с консьюмер-группами**:
   - Создайте топик с 4 партициями
   - Запустите 2 консьюмера в одной группе
   - Отправьте сообщения в топик
   - Наблюдайте, как сообщения распределяются между консьюмерами
   - Добавьте третий консьюмер и наблюдайте ребалансировку

3. **Работа со схемами Avro**:
   - Создайте новую схему Avro для сообщений
   - Отправьте сообщения, соответствующие этой схеме
   - Измените схему и отправьте новые сообщения
   - Проверьте совместимость версий схемы

4. **Тестирование отказоустойчивости**:
   - Отправьте сообщения в топик
   - Остановите и перезапустите брокер Kafka
   - Проверьте, что сообщения не потерялись

## Полезные ресурсы

- [Официальная документация Apache Kafka](https://kafka.apache.org/documentation/)
- [Confluent Kafka Documentation](https://docs.confluent.io/platform/current/overview.html)
- [Kafka Python Client](https://kafka-python.readthedocs.io/)
- [Schema Registry Documentation](https://docs.confluent.io/platform/current/schema-registry/index.html)

## Устранение неполадок

### Проблемы с подключением к Kafka

Если у вас возникают проблемы с подключением к Kafka, проверьте следующее:

1. Убедитесь, что все контейнеры запущены:
   ```
   docker compose ps
   ```

2. Проверьте логи Kafka:
   ```
   docker compose logs kafka
   ```

3. Проверьте доступность порта 9092:
   ```
   telnet localhost 9092
   ```

### Проблемы с Schema Registry

Если возникают проблемы при работе со Schema Registry:

1. Проверьте логи Schema Registry:
   ```
   docker compose logs schema-registry
   ```

2. Убедитесь, что Schema Registry доступен:
   ```
   curl -X GET http://localhost:8081/subjects
   ```

## Остановка тренажера

Для остановки всех контейнеров выполните:
```
docker compose down
```

Данные Kafka и ZooKeeper хранятся в именованных томах и сохраняются после `docker compose down`.

Для полной очистки (включая удаление данных):
```
docker compose down -v
```

## Проверка исправлений

После запуска стенда выполните из корня репозитория; проверка перезапуска остановит и снова запустит брокер Kafka:

```bash
docker exec python-tools python -m unittest discover -s tests -v
docker exec python-tools python tests/smoke.py
docker exec python-tools python tests/assignments.py
python3 tests/restart_broker.py
```

Проверки создают временные топики и удаляют их после завершения.
`assignments.py` выполняет все шесть готовых сценариев и практические задания 1–3, включая команды, напечатанные для ручного запуска.
`restart_broker.py` проверяет задание 4: останавливает Kafka, запускает снова и проверяет сохранность десяти сообщений.

Для клиентов на компьютере используйте `localhost:9092`, внутри Docker-сети `kafka:9092`.
Порты стенда доступны только на локальном компьютере
Топики нужно создавать явно: автоматическое создание отключено
Примеры команд управления топиками можно выполнять по отдельности; удаляйте топик только после работы с ним

`producer.py --message` принимает JSON или обычный текст, в том числе пустую строку.
`--from-beginning` выбирает начало только при отсутствии сохранённого смещения группы
Обычный `consumer.py` без `--group` не сохраняет позицию чтения между запусками
Для повторного чтения Avro используйте новую группу: у существующей группы сохраняется позиция чтения
Для повторного чтения JSON с нулевого смещения выполните:

```bash
docker exec -it python-tools python test_recovery.py --topic my-topic --group replay-example --mode recover --offset 0
```

Остановите непрерывное чтение сочетанием Ctrl+C
Сценарии `consumer_groups` и `rebalancing` печатают команды для ручной работы в нескольких терминалах.
Генератор Avro поддерживает string, int, long, float, double, boolean, null и их union; для вложенных схем передавайте готовые данные через `--data`.


## Другие проекты

[RabbitMQ для QA](https://github.com/ik-slyadnev/rabbit_mq_example): тренажёр очередей и обменников с Flask REST API, сохранением истории сообщений и восстановлением потребителей
