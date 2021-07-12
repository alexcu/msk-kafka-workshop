echo "consume from topic name"
read topic_name

if [ -z "$topic_name" ]
then
  echo "topic name is empty"
  exit 1
fi

echo "which consumer group this consumer belongs to?"
read consumer_group

echo "do you to read messages from the beginning?"
read from_begin

if [ -z "$consumer_group" ]
  then
    if [ "$from_begin" = "Y" ] || [ "$from_begin" = "y" ] || [ "$from_begin" = "yes" ]
    then
      kafka-console-consumer.sh --bootstrap-server localhost:9092 \
      --topic $topic_name \
      --from-beginning
    else
      kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic $topic_name
    fi
else
  if [ "$from_begin" = "Y" ] || [ "$from_begin" = "y" ] || [ "$from_begin" = "yes" ]
  then
     kafka-console-consumer.sh --bootstrap-server localhost:9092 \
        --topic $topic_name \
        --group $consumer_group \
        --from-beginning
  else
    kafka-console-consumer.sh --bootstrap-server localhost:9092 \
        --topic $topic_name \
        --group $consumer_group
  fi
fi