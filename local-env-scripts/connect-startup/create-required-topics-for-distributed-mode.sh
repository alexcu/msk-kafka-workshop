#!/bin/zsh

# config.storage.topic=connect-configs
kafka-topics.sh --create --bootstrap-server localhost:9092 \
  --topic connect-configs \
  --replication-factor 3 \
  --partitions 1 \
  --config cleanup.policy=compact

# offset.storage.topic=connect-offsets
kafka-topics --create --bootstrap-server localhost:9092 \
  --topic connect-offsets \
  --replication-factor 3 \
  --partitions 50 \
  --config cleanup.policy=compact

# status.storage.topic=connect-status
kafka-topics --create --bootstrap-server localhost:9092 \
  --topic connect-status \
  --replication-factor 3 \
  --partitions 10 \
  --config cleanup.policy=compact

