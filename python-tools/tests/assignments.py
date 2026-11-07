"""Run six README scenarios and practical tasks 1-3 inside python-tools."""
from collections import Counter
import copy
from contextlib import closing
import json
import logging
from pathlib import Path
import re
import shlex
import subprocess
import sys
import tempfile
import time
from uuid import uuid4

sys.path.insert(0, '/app')
from kafka import KafkaConsumer
from kafka.admin import KafkaAdminClient, NewTopic
from confluent_kafka.schema_registry import SchemaRegistryClient, Schema
from monitoring import KafkaMonitor

logging.getLogger().setLevel(logging.ERROR)
admin = KafkaAdminClient(bootstrap_servers=['kafka:9092'])
registry = SchemaRegistryClient({'url': 'http://schema-registry:8081'})
created_topics = set()
created_subjects = set()


def run(*args, success=True):
    result = subprocess.run([sys.executable, *args], text=True, capture_output=True, timeout=120)
    assert (result.returncode == 0) == success, (args, result.stdout, result.stderr)
    return result.stdout


def wait(check, label):
    deadline = time.monotonic() + 50
    while time.monotonic() < deadline:
        if check():
            return
        time.sleep(0.5)
    raise AssertionError('Timed out: ' + label)


def group_scenario(scenario):
    output = run('test_scenarios.py', '--scenario', scenario)
    topic = re.search(r'--create (\S+)', output).group(1)
    created_topics.add(topic)
    commands = re.findall(r'docker exec -it python-tools (python (?:consumer|producer)\.py[^\n]*)', output)
    assert len(commands) >= 4, output
    workers = []
    monitor = KafkaMonitor()
    sent = 0
    group = re.search(r'--group (\S+)', output).group(1)

    def stable(count):
        info = monitor.get_consumer_group_metrics(group)
        if info['state'] != 'Stable' or info['members_count'] != count:
            return False
        partitions = [p for member in info['members'] for p in member['partitions']]
        expected = 4 if scenario == 'rebalancing' else 3
        assert len(partitions) == len(set(partitions)) == expected, info
        assert all(member['partitions'] for member in info['members']), info
        if expected == 4 and count in (2, 3):
            assert sorted(len(m['partitions']) for m in info['members']) == ([2, 2] if count == 2 else [1, 1, 2]), info
        return True

    def delivered():
        info = monitor.get_consumer_group_metrics(group)
        return sum(v['committed_offset'] for v in info['offsets'].values()) == sent

    try:
        for command in commands:
            args = shlex.split(command)[1:]
            if args[0] == 'consumer.py':
                log = tempfile.TemporaryFile(mode='w+')
                process = subprocess.Popen([sys.executable, '-u', *args], stdout=log, stderr=log)
                workers.append((process, log))
                wait(lambda: stable(len(workers)), 'group assignment')
            else:
                run(*args)
                sent += int(args[args.index('--count') + 1])
                wait(delivered, 'all messages committed')
        if scenario == 'rebalancing':
            workers[0][0].terminate()
            workers[0][0].wait(timeout=20)
            wait(lambda: stable(2), 'rebalance after consumer leaves')
            run('producer.py', '--topic', topic, '--count', '4', '--interval', '0')
            sent += 4
            wait(delivered, 'delivery after rebalance')
    finally:
        for process, log in workers:
            if process.poll() is None:
                process.terminate()
        received = []
        for process, log in workers:
            process.wait(timeout=20)
            assert process.returncode == 0
            log.seek(0)
            records = re.findall(r'Топик: (\S+), Партиция: (\d+), Смещение: (\d+)', log.read())
            assert records, 'Consumer received no messages'
            received.extend(records)
            log.close()
        monitor.admin_client.close()
    assert len(received) == len(set(received)) == sent, (sent, len(received), len(set(received)))
    print(f'PASS {scenario}: 3 real consumers, {sent} messages, no duplicates; assignments and rebalance verified', flush=True)


try:
    for scenario in ('basic_producer_consumer', 'multiple_partitions', 'avro_schema', 'error_handling'):
        output = run('test_scenarios.py', '--scenario', scenario)
        created_topics.update(re.findall(r'--create (\S+)', output))
        if scenario == 'basic_producer_consumer':
            assert output.count('Значение:') == 5
        elif scenario == 'multiple_partitions':
            assert output.count('Значение:') == 9
        elif scenario == 'avro_schema':
            assert output.count('Значение:') == 4
            created_subjects.update(t + '-value' for t in re.findall(r'--create (\S+)', output))
        print('PASS ready scenario: ' + scenario, flush=True)

    topic = 'assignment-' + uuid4().hex
    created_topics.add(topic)
    run('topic_manager.py', '--create', topic, '--partitions', '3', '--replication', '1')
    assert topic in run('topic_manager.py', '--list')
    run('topic_manager.py', '--describe', topic)
    run('producer.py', '--topic', topic, '--count', '10', '--interval', '0')
    with closing(KafkaConsumer(topic, bootstrap_servers=['kafka:9092'], auto_offset_reset='earliest',
                       consumer_timeout_ms=3000)) as consumer:
        messages = list(consumer)
    assert len(messages) == 10
    assert sorted(Counter(m.partition for m in messages).values()) == [3, 3, 4]
    output = run('consumer.py', '--topic', topic, '--from-beginning', '--limit', '10')
    assert output.count('Значение:') == 10
    print('PASS practice 1: 3 partitions, 10 messages, distribution 4/3/3', flush=True)
    group_scenario('consumer_groups')
    group_scenario('rebalancing')

    print('PASS practice 2: 4 partitions, two active consumers received messages, third consumer joined and received messages', flush=True)
    topic = 'schema-evolution-' + uuid4().hex
    subject = topic + '-value'
    created_topics.add(topic)
    created_subjects.add(subject)
    admin.create_topics([NewTopic(topic, 1, 1)])
    schema = {'type': 'record', 'name': 'PracticeEvent', 'fields': [{'name': 'id', 'type': 'int'}]}
    with tempfile.TemporaryDirectory() as directory:
        path = Path(directory)
        for version in (1, 2):
            if version == 2:
                schema['fields'].append({'name': 'note', 'type': 'string', 'default': ''})
                assert registry.test_compatibility(subject, Schema(json.dumps(schema), 'AVRO'))
            (path / 'event.avsc').write_text(json.dumps(schema))
            (path / 'event.json').write_text(json.dumps({'id': version} if version == 1 else {'id': version, 'note': 'new field'}))
            run('avro_producer.py', '--topic', topic, '--schema', str(path / 'event.avsc'), '--data', str(path / 'event.json'), '--interval', '0')
            if version == 1:
                registry.set_compatibility(subject_name=subject, level='BACKWARD')
        assert registry.get_versions(subject) == [1, 2]
        incompatible = copy.deepcopy(schema)
        incompatible['fields'][0]['type'] = 'string'
        assert not registry.test_compatibility(subject, Schema(json.dumps(incompatible), 'AVRO'))
        (path / 'bad.avsc').write_text(json.dumps(incompatible))
        (path / 'bad.json').write_text(json.dumps({'id': 'bad', 'note': 'incompatible'}))
        run('avro_producer.py', '--topic', topic, '--schema', str(path / 'bad.avsc'), '--data', str(path / 'bad.json'), success=False)
        assert registry.get_versions(subject) == [1, 2]
    output = run('avro_consumer.py', '--topic', topic, '--group', topic, '--from-beginning', '--limit', '2')
    assert '"id": 1' in output and '"id": 2' in output and 'new field' in output
    print('PASS practice 3: schema v1/v2 published and read, BACKWARD accepted, incompatible change rejected', flush=True)
finally:
    existing = set(admin.list_topics())
    if existing & created_topics:
        admin.delete_topics(list(existing & created_topics))
    for subject in created_subjects:
        if subject in registry.get_subjects():
            registry.delete_subject(subject)
    admin.close()
