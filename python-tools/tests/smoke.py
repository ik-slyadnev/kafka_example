"""Run in python-tools: python tests/smoke.py."""
from collections import Counter
import json
import logging
import subprocess
import sys
import time
from uuid import uuid4

from kafka import KafkaConsumer, KafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic

# The tools live one directory above this test script
sys.path.insert(0, '/app')
from monitoring import KafkaMonitor

logging.getLogger().setLevel(logging.ERROR)
prefix = 'smoke-' + uuid4().hex
json_topic, avro_topic, null_topic = [prefix + suffix for suffix in ('-json', '-avro', '-null')]
admin = KafkaAdminClient(bootstrap_servers=['kafka:9092'])


def run(*args, success=True, timeout=30):
    result = subprocess.run([sys.executable, *args], text=True, capture_output=True, timeout=timeout)
    assert (result.returncode == 0) == success, (args, result.stdout, result.stderr)
    return result.stdout


try:
    admin.create_topics([NewTopic(json_topic, 3, 1), NewTopic(avro_topic, 1, 1), NewTopic(null_topic, 1, 1)])
    run('topic_manager.py', '--describe', json_topic)
    run('producer.py', '--topic', json_topic, '--message', 'plain text', '--count', '9', '--interval', '0')
    consumer = KafkaConsumer(json_topic, bootstrap_servers=['kafka:9092'], auto_offset_reset='earliest',
                             consumer_timeout_ms=5000, value_deserializer=lambda value: json.loads(value))
    try:
        messages = list(consumer)
        assert len(messages) == 9, len(messages)
        assert Counter(m.partition for m in messages) == {0: 3, 1: 3, 2: 3}
        assert all(m.value == 'plain text' for m in messages)
    finally:
        consumer.close()
    run('producer.py', '--topic', null_topic, '--message', '', '--key', '', '--interval', '0')
    producer = KafkaProducer(bootstrap_servers=['kafka:9092'])
    try:
        producer.send(null_topic, value=None, key=b'tombstone').get(timeout=10)
    finally:
        producer.close()
    output = run('consumer.py', '--topic', null_topic, '--from-beginning', '--limit', '2')
    assert 'null' in output and '""' in output
    run('avro_producer.py', '--topic', avro_topic, '--schema', 'schemas/user_event.avsc',
        '--data', 'data/sample_events.json', '--interval', '0')
    output = run('avro_consumer.py', '--topic', avro_topic, '--group', prefix + '-avro-group',
                 '--from-beginning', '--limit', '4')
    assert output.count('Значение:') == 4, output
    run('avro_producer.py', '--topic', prefix + '-missing', '--schema', 'schemas/user_event.avsc', success=False)

    process = subprocess.Popen([sys.executable, 'consumer.py', '--topic', json_topic,
                                '--group', prefix, '--from-beginning'], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    monitor = KafkaMonitor()
    try:
        deadline = time.monotonic() + 25
        while time.monotonic() < deadline:
            metrics = monitor.collect_all_metrics(topic=json_topic, group_id=prefix)
            if metrics['consumer_groups'][prefix]['state'] == 'Stable' and metrics['consumer_groups'][prefix]['members_count'] == 1:
                break
            time.sleep(0.5)
        else:
            raise AssertionError('Consumer did not join group')
        group = metrics['consumer_groups'][prefix]
        assert len(group['members'][0]['partitions']) == 3, group
        json.dumps(monitor.collect_all_metrics())
    finally:
        process.terminate()
        process.communicate(timeout=15)
        assert process.returncode == 0, process.returncode
        monitor.admin_client.close()

    run('test_consumer_groups.py', '--topic', json_topic, '--group', prefix + '-rebalance',
        '--consumers', '2', '--duration', '1')
    process = subprocess.Popen([sys.executable, 'test_recovery.py', '--topic', json_topic, '--group', prefix + '-recover',
                                '--mode', 'recover', '--offset', '1'], stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    try:
        process.communicate(timeout=4)
        raise AssertionError('Recovery consumer stopped unexpectedly')
    except subprocess.TimeoutExpired:
        process.terminate()
        output = process.communicate(timeout=15)[1].decode()
        assert 'смещение=1' in output and 'смещение=0,' not in output, output
        assert process.returncode == 0
    for script, args in [('producer.py', ['--topic', json_topic, '--count', '0']),
                         ('consumer.py', ['--topic', json_topic, '--limit', '0']),
                         ('avro_consumer.py', ['--topic', avro_topic, '--limit', '-1']),
                         ('monitoring.py', ['--interval', '0'])]:
        run(script, *args, success=False)
    print('PASS: topics, JSON/text, partitions, empty key, tombstone, Avro, delivery failure, group metrics, signals, rebalancing, offset recovery, CLI validation')
finally:
    admin.delete_topics([json_topic, avro_topic, null_topic])
    admin.close()
