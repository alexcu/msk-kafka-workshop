#!/bin/zsh
echo "provide the log file that you want to read"
read log_file

kafka-dump-log.sh --files $log_file
