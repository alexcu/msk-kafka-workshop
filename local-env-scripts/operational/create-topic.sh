#!/bin/zsh
echo "topic name:"
read topic_name

echo "number of partitions:"
read partitions

echo "number of replicas:"
read replicas

echo "segment size for the topic (1073741824 bytes = 1 GB) | (1000000 bytes = 1MB)"
read segment_size

echo "number of minimum inync replicas: [(0 = default) or (a number <= number of replicas)]"
read min_insync_replicas

if [ $min_insync_replicas = 0 ];
then
  kafka-topics.sh --bootstrap-server localhost:9092,localhost:9093 \
  --topic $topic_name \
  --partitions $partitions \
  --replication-factor $replicas \
  --config segment.bytes=$segment_size \
  --create
else
  kafka-topics.sh --bootstrap-server localhost:9092,localhost:9093 \
  --topic $topic_name \
  --partitions $partitions \
  --replication-factor $replicas \
  --config segment.bytes=$segment_size \
  --config min.insync.replicas=$min_insync_replicas \
  --create
fi
