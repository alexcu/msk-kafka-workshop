#!/bin/zsh

echo "do you want to List or Describe consumer group? (L/D)"
read user_pref

if [ "$user_pref" = "L" ] || [ "$user_pref" = "l" ];
then
  kafka-consumer-groups.sh --bootstrap-server localhost:9092 \
  --list
elif [ "$user_pref" = "D" ] || [ "$user_pref" = "d" ];
then

  echo "pls enter Consumer Group name:"
  read consumer_group_name

  kafka-consumer-groups.sh --bootstrap-server localhost:9092 \
  --describe \
  --group $consumer_group_name

else
  echo "could not understand your input, pls mention either L or D."
fi