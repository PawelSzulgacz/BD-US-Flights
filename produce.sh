source ./env.sh
java -cp /usr/lib/kafka/libs/*:KafkaProducer.jar com.example.bigdata.FlightsProducer $INPUT_DIR 15 $KAFKA_TOPIC ${CLUSTER_NAME}-w-0:9092 s