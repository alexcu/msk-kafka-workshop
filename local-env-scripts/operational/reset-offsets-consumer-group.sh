#!/bin/zsh

echo "enter topic name"
read topic_name

if [ -z "$topic_name" ]
then
  echo "topic name is empty"
  exit 1
fi

echo "enter consumer group name"
read group_name

if [ -z "$group_name" ]
then
  echo "Consumer group name is empty"
  exit 1
fi


echo "enter reset option"
echo "choose one of the following:"
echo "1 for to-datetime (format:    yyyy-MM-ddTHH:mm:ss.xxx e.g. 2017-08-04T00:00:00.000)"
echo "2 for by-duration   (format: P(n)Y(n)M(n)DT(n)H(n)M(n)S  e.g. P3Y6M4DT12H30M5S  duration of three years, six months, four days, twelve hours, thirty minutes, and five seconds."
echo "3 for to-earliest"
echo "4 for to-latest"
echo "5 for shift-by"
echo "6 for from-file   (format:  provide a csv file, each line should:  topicName,partitionNumber,offset)"
echo "7 for to-current"

read user_pref
if [ -z "$user_pref" ]
then
  echo "You haven't selected any option."
  exit 1
fi


if [ "$user_pref" = 1 ];
then
    echo "option $user_pref is selected"
    echo "enter date time yyyy-MM-ddTHH:mm:ss.xxx"
    read date_time

    kafka-consumer-groups.sh --bootstrap-server localhost:9092 \
    --topic $topic_name \
    --group $group_name \
    --reset-offsets \
    --to-datetime $date_time \
    --execute
elif [ "$user_pref" = 2 ];
then
  echo "option $user_pref is selected"
  echo "enter duration P(n)Y(n)M(n)DT(n)H(n)M(n)S e.g. PT15M (15 minutes), P2DT3H4M (2 days, 3 hours and 4 minutes)"
  read period_details

  kafka-consumer-groups.sh --bootstrap-server localhost:9092 \
  --topic $topic_name \
  --group $group_name \
  --reset-offsets \
  --by-duration $period_details \
  --execute
elif [ "$user_pref" = 3 ];
then
    echo "option $user_pref is selected"
    echo "do you want to reset offset for a specific partition? if yes, enter the partition number e.g. 0 or 1 or 2 etc."
    read partition_number

    if [ -z $partition_number ];
    then
        kafka-consumer-groups.sh --bootstrap-server localhost:9092 \
        --topic $topic_name \
        --group $group_name \
        --reset-offsets \
        --to-earliest \
        --execute
    else
        kafka-consumer-groups.sh --bootstrap-server localhost:9092 \
        --topic $topic_name:$partition_number \
        --group $group_name \
        --reset-offsets \
        --to-earliest \
        --execute
    fi
elif [ "$user_pref" = 4 ];
then
  echo "option $user_pref is selected"

  kafka-consumer-groups.sh --bootstrap-server localhost:9092 \
  --topic $topic_name \
  --group $group_name \
  --reset-offsets \
  --to-latest \
  --execute

elif [ "$user_pref" = 5 ];
then
  echo "option $user_pref is selected"
  echo "how many offsets you want to shift by (number)?"
  read shift_offsets

  kafka-consumer-groups.sh --bootstrap-server localhost:9092 \
  --topic $topic_name \
  --group $group_name \
  --reset-offsets \
  --shift-by $shift_offsets \
  --execute
elif [ "$user_pref" = 6 ];
then
  echo "option $user_pref is selected"
  echo "file name:"
  read file_name

  kafka-consumer-groups.sh --bootstrap-server localhost:9092 \
  --topic $topic_name \
  --group $group_name \
  --reset-offsets \
  --from-file $file_name \
  --execute
elif [ "$user_pref" = 7 ];
then
  echo "option $user_pref is selected"
fi

