# Stop Kafka Connect
echo "Stopping Kafka Connect..."
pkill -f 'connect-standalone'

# Stop Kafka Broker
echo "Stopping Kafka broker..."
$KAFKA_HOME/bin/kafka-server-stop.sh
sleep 3

# Stop Zookeeper
echo "Stopping Zookeeper..."
$KAFKA_HOME/bin/zookeeper-server-stop.sh
sleep 3

echo "All services stopped: Kafka Connect, Kafka, and Zookeeper."
