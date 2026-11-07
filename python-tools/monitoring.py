#!/usr/bin/env python3
from kafka import KafkaAdminClient, KafkaConsumer, TopicPartition
from contextlib import closing
import json
import time
import logging
from argparse import ArgumentParser
import signal
import sys

# Настройка логирования
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class KafkaMonitor:
    def __init__(self, bootstrap_servers=['kafka:9092']):
        self.bootstrap_servers = bootstrap_servers
        self.admin_client = KafkaAdminClient(bootstrap_servers=bootstrap_servers)
        
    def get_topic_metrics(self, topic):
        """Получение метрик топика"""
        consumer = KafkaConsumer(
            bootstrap_servers=self.bootstrap_servers,
            consumer_timeout_ms=1000
        )
        
        metrics = {
            "partitions": {},
            "total_messages": 0
        }
        
        with closing(consumer):
            # Получение информации о партициях
            partitions = consumer.partitions_for_topic(topic)
            if not partitions:
                logger.warning(f"Топик {topic} не найден")
                return metrics

            for partition in partitions:
                tp = TopicPartition(topic, partition)
                # Получение начального и конечного смещения
                beginning_offset = consumer.beginning_offsets([tp])[tp]
                end_offset = consumer.end_offsets([tp])[tp]

                messages_count = end_offset - beginning_offset
                metrics["partitions"][partition] = {
                    "beginning_offset": beginning_offset,
                    "end_offset": end_offset,
                    "messages": messages_count
                }
                metrics["total_messages"] += messages_count

            return metrics
            
        
    def get_consumer_group_metrics(self, group_id):
        """Метрики группы потребителей: состояние, участники, смещения и отставание (lag)"""
        info = self.admin_client.describe_consumer_groups([group_id])[0]
        members = []
        for m in info.members:
            assigned = []
            if m.member_assignment:
                for topic, partitions in m.member_assignment.assignment:
                    assigned += [f"{topic}-{p}" for p in partitions]
            members.append({"client_id": m.client_id, "host": m.client_host, "partitions": assigned})

        committed = self.admin_client.list_consumer_group_offsets(group_id)
        existing_topics = set(self.admin_client.list_topics())
        committed = {tp: meta for tp, meta in committed.items()
                     if tp.topic in existing_topics and meta.offset >= 0}
        offsets = {}
        if committed:
            with closing(KafkaConsumer(bootstrap_servers=self.bootstrap_servers)) as consumer:
                end_offsets = consumer.end_offsets(list(committed.keys()))
            for tp, meta in sorted(committed.items()):
                offsets[f"{tp.topic}-{tp.partition}"] = {
                    "committed_offset": meta.offset,
                    "end_offset": end_offsets[tp],
                    "lag": end_offsets[tp] - meta.offset,
                }

        return {
            "state": info.state,
            "members_count": len(members),
            "members": members,
            "offsets": offsets,
            "total_lag": sum(o["lag"] for o in offsets.values()),
        }

    def collect_all_metrics(self, topic=None, group_id=None):
        """Сбор всех доступных метрик"""
        metrics = {
            "timestamp": time.time(),
            "topics": {},
            "consumer_groups": {}
        }
        
        # Сбор метрик топиков
        if topic:
            topics = [topic]
        else:
            topics = self.admin_client.list_topics()
            
        for t in topics:
            metrics["topics"][t] = self.get_topic_metrics(t)
            
        # Сбор метрик групп потребителей
        if group_id:
            groups = [group_id]
        else:
            groups = [group_id for group_id, protocol in self.admin_client.list_consumer_groups()
                      if protocol in ("consumer", "")]
            
        for g in groups:
            metrics["consumer_groups"][g] = self.get_consumer_group_metrics(g)
            
        return metrics

def main():
    parser = ArgumentParser(description='Мониторинг Kafka')
    parser.add_argument('--topic', help='Имя топика для мониторинга')
    parser.add_argument('--group', help='ID группы потребителей для мониторинга')
    parser.add_argument('--interval', type=int, default=60,
                       help='Интервал сбора метрик в секундах')
    
    args = parser.parse_args()
    if args.interval <= 0:
        parser.error("--interval должен быть больше нуля")
    
    def signal_handler(signum, frame):
        logger.info("Получен сигнал остановки, завершение работы...")
        sys.exit(0)
    
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    monitor = KafkaMonitor()
    
    try:
        while True:
            metrics = monitor.collect_all_metrics(args.topic, args.group)
            print(json.dumps(metrics, indent=2, ensure_ascii=False, default=str))
            print(f"Следующий сбор через {args.interval} сек. Для остановки нажми Ctrl+C")
            time.sleep(args.interval)
    except KeyboardInterrupt:
        logger.info("Мониторинг остановлен пользователем")
    finally:
        monitor.admin_client.close()

if __name__ == "__main__":
    main()