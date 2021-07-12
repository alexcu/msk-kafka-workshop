echo "first topic name"
read topic_name_1

echo "second topic name"
read topic_name_2

echo "which consumer group this consumer belongs to?"
read consumer_group

echo "do you to read messages from the beginning?"
read from_begin


if [ -z "$topic_name_1" ] || [ -z "$topic_name_2" ]
then
  echo "first topic and/or second topic name is an empty string"
  exit 1
fi

if [ "$from_begin" = "Y" ] || [ "$from_begin" = "y" ] || [ "$from_begin" = "yes" ]
then
  kafka-console-consumer.sh --bootstrap-server localhost:9092 \
  --whitelist "$topic_name_1|$topic_name_2" \
  --from-beginning --group $consumer_group
else
  kafka-console-consumer.sh --bootstrap-server localhost:9092 \
  --whitelist $topic_name_1|$topic_name_2 \
  --group $consumer_group
fi