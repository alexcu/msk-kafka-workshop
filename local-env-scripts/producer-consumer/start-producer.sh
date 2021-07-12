#!/bin/zsh

echo "topic name"
read topic_name


if [ -z "$topic_name" ]
then
  echo "topic name is empty"
  exit 1
fi

kafka-console-producer.sh --bootstrap-server localhost:9092 --topic $topic_name
