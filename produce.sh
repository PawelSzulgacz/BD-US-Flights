#!/bin/bash

source ./env.sh
kafka-topics.sh --bootstrap-server ${CLUSTER_NAME}-w-1:9092 --delete --topic $KAFKA_TOPIC
kafka-topics.sh --bootstrap-server ${CLUSTER_NAME}-w-1:9092 --create --replication-factor 1 --partitions 1 --topic $KAFKA_TOPIC
hadoop fs -copyToLocal gs://${BUCKET_NAME}/projekt2/TestData
hadoop fs -copyToLocal gs://${BUCKET_NAME}/projekt2/airports/airports.csv
java -cp /usr/lib/kafka/libs/*:KafkaProducer.jar com.example.bigdata.FlightsProducer $INPUT_DIR 15 $KAFKA_TOPIC ${CLUSTER_NAME}-w-0:9092 s