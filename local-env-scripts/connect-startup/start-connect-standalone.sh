#!/bin/zsh
connect-standalone.sh $KAFKA_HOME/config/connect-standalone.properties \
          $KAFKA_HOME/config/connect-file-source.properties \
          $KAFKA_HOME/config/connect-file-sink.properties