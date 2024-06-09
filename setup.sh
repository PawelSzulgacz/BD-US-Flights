source ./env.sh

echo "Creating Kafka topic $KAFKA_TOPIC"
kafka-topics.sh --bootstrap-server ${CLUSTER_NAME}-w-1:9092 --delete --topic $KAFKA_TOPIC
kafka-topics.sh --bootstrap-server ${CLUSTER_NAME}-w-1:9092 --create --replication-factor 1 --partitions 1 --topic $KAFKA_TOPIC
echo "Downloading data from bucket"
hadoop fs -copyToLocal  gs://"${BUCKET_NAME}"/projekt2/${INPUT_DIR}
hadoop fs -copyToLocal  gs://"${BUCKET_NAME}"/projekt2/${LOCAL_DIR}/airports.csv
echo "Downloading dependencies"
wget https://repo1.maven.org/maven2/org/apache/flink/flink-clients/1.16.1/flink-clients-1.16.1.jar
wget https://repo1.maven.org/maven2/org/apache/flink/flink-connector-kafka/1.15.4/flink-connector-kafka-1.15.4.jar
echo "Done"