import json
import unittest
from unittest.mock import Mock, patch
import producer
import consumer
import avro_producer
import test_consumer_groups
import avro_consumer
import monitoring
import test_scenarios
from kafka import TopicPartition
from kafka.structs import OffsetAndMetadata


class ToolTests(unittest.TestCase):
    def test_avro_consumer_accepts_default_writer_schema(self):
        with patch.object(avro_consumer, 'DeserializingConsumer') as consumer:
            avro_consumer.create_avro_consumer('topic')
        self.assertIsNotNone(consumer.call_args.args[0]['value.deserializer'])

    def test_consumer_reads_raw_text_json_and_tombstones(self):
        self.assertEqual(consumer.deserialize_value(b'raw text'), 'raw text')
        self.assertEqual(consumer.deserialize_value(b'{"id": 1}'), {'id': 1})
        self.assertIsNone(consumer.deserialize_value(None))

    def test_avro_delivery_failure_raises(self):
        from confluent_kafka import KafkaException, KafkaError
        with self.assertRaises(KafkaException):
            avro_producer.delivery_report(KafkaError(KafkaError._MSG_TIMED_OUT), None)

    def test_worker_failure_reaches_main_thread(self):
        with patch.object(test_consumer_groups, 'create_consumer', side_effect=RuntimeError('broker failed')):
            with self.assertRaises(RuntimeError):
                test_consumer_groups.test_consumer_group_rebalancing('topic', 'group', 1, 0)

    def test_empty_key_is_preserved(self):
        client = Mock()
        producer.send_message(client, 'topic', 'text', key='')
        self.assertEqual(client.send.call_args.kwargs['key'], b'')

    def test_group_list_tuples_are_unpacked_and_json_serializable(self):
        monitor = monitoring.KafkaMonitor.__new__(monitoring.KafkaMonitor)
        monitor.admin_client = Mock()
        monitor.admin_client.list_topics.return_value = []
        monitor.admin_client.list_consumer_groups.return_value = [('qa-group', 'consumer')]
        monitor.get_consumer_group_metrics = Mock(return_value={'total_lag': 0})
        metrics = monitor.collect_all_metrics()
        monitor.get_consumer_group_metrics.assert_called_once_with('qa-group')
        json.dumps(metrics)

    def test_deleted_topics_are_excluded_from_group_lag(self):
        monitor = monitoring.KafkaMonitor.__new__(monitoring.KafkaMonitor)
        monitor.admin_client = Mock()
        monitor.admin_client.describe_consumer_groups.return_value = [Mock(members=[], state='Empty')]
        monitor.admin_client.list_consumer_group_offsets.return_value = {
            TopicPartition('deleted', 0): OffsetAndMetadata(3, '')}
        monitor.admin_client.list_topics.return_value = ['other']
        result = monitor.get_consumer_group_metrics('group')
        self.assertEqual(result['offsets'], {})
        self.assertEqual(result['total_lag'], 0)

    def test_missing_topic_closes_consumer(self):
        monitor = monitoring.KafkaMonitor.__new__(monitoring.KafkaMonitor)
        monitor.bootstrap_servers = ['kafka:9092']
        client = Mock()
        client.partitions_for_topic.return_value = None
        with patch.object(monitoring, 'KafkaConsumer', return_value=client):
            self.assertEqual(monitor.get_topic_metrics('missing')['total_messages'], 0)
        client.close.assert_called_once()

    def test_metrics_failure_closes_consumer(self):
        monitor = monitoring.KafkaMonitor.__new__(monitoring.KafkaMonitor)
        monitor.bootstrap_servers = ['kafka:9092']
        client = Mock()
        client.partitions_for_topic.return_value = {0}
        client.beginning_offsets.side_effect = RuntimeError('broker disconnected')
        with patch.object(monitoring, 'KafkaConsumer', return_value=client):
            with self.assertRaises(RuntimeError):
                monitor.get_topic_metrics('topic')
        client.close.assert_called_once()

    def test_scenario_stops_on_failed_command(self):
        with self.assertRaises(Exception):
            test_scenarios.run_command("python -c 'raise SystemExit(7)'")


if __name__ == '__main__':
    unittest.main()
