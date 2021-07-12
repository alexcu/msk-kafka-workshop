#!/bin/zsh
echo "topic name: "
read topic_name

kafka-topics.sh --bootstrap-server localhost:9092 \
--topic $topic_name \
--describe