# Kafka Тренажер для Manual QA

Этот проект представляет собой локальный тренажер для отработки навыков работы с Apache Kafka для специалистов по ручному тестированию (Manual QA). Тренажер позволяет изучить основные концепции Kafka и отработать различные сценарии работы с этой технологией.

## Содержание

- [Требования](#требования)
- [Установка и запуск](#установка-и-запуск)
- [Компоненты тренажера](#компоненты-тренажера)
- [Основные команды](#основные-команды)
- [Тестовые сценарии](#тестовые-сценарии)
- [Полезные ресурсы](#полезные-ресурсы)

## Требования

Для работы с тренажером вам понадобится:

- Docker и Docker Compose
- Базовые знания работы с терминалом
- Базовое понимание концепций Kafka

## Установка и запуск

1. Клонируйте репозиторий:
   ```
   git clone <url-репозитория>
   cd kafka-qa-trainer
   ```

2. Запустите контейнеры с помощью Docker Compose:
   ```
   docker-compose up -d
   ```

3. Проверьте, что все контейнеры запущены:
   ```
   docker-compose ps
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

Удаление топика:
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
docker exec -it python-tools python consumer.py --topic my-topic --from-beginning
```

### Работа с Avro и Schema Registry

Отправка сообщений с использованием Avro:
```
docker exec -it python-tools python avro_producer.py --topic avro-topic --schema schemas/user_event.avsc --data data/sample_events.json
```

Чтение сообщений в формате Avro:
```
docker exec -it python-tools python avro_consumer.py --topic avro-topic --from-beginning
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
   docker-compose ps
   ```

2. Проверьте логи Kafka:
   ```
   docker-compose logs kafka
   ```

3. Проверьте доступность порта 9092:
   ```
   telnet localhost 9092
   ```

### Проблемы с Schema Registry

Если возникают проблемы при работе со Schema Registry:

1. Проверьте логи Schema Registry:
   ```
   docker-compose logs schema-registry
   ```

2. Убедитесь, что Schema Registry доступен:
   ```
   curl -X GET http://localhost:8081/subjects
   ```

## Остановка тренажера

Для остановки всех контейнеров выполните:
```
docker-compose down
```

Для полной очистки (включая удаление данных):
```
docker-compose down -v
```