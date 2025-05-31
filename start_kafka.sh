#!/bin/bash

# Set paths to Kafka installation and config files
ZOOKEEPER_CONFIG="$KAFKA_HOME/config/zookeeper.properties"
KAFKA_CONFIG="$KAFKA_HOME/config/server.properties"
CONNECT_CONFIG="$KAFKA_HOME/config/connect-standalone.properties"

# Start Zookeeper
echo "Starting Zookeeper..."
$KAFKA_HOME/bin/zookeeper-server-start.sh -daemon $ZOOKEEPER_CONFIG
sleep 5

# Start Kafka broker
echo "Starting Kafka broker..."
$KAFKA_HOME/bin/kafka-server-start.sh -daemon $KAFKA_CONFIG
sleep 5

# Start Kafka Connect with multiple sink configs
echo "Starting Kafka Connect..."
$KAFKA_HOME/bin/connect-standalone.sh -daemon $CONNECT_CONFIG \
    $KAFKA_HOME/config/sink-configs/pin_data_pin_sink.properties \
    $KAFKA_HOME/config/sink-configs/pin_data_geo_sink.properties \
    $KAFKA_HOME/config/sink-configs/pin_data_user_sink.properties
sleep 5

echo "All services started: Zookeeper, Kafka, and Kafka Connect."
