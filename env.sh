#!/bin/bash

export BUCKET_NAME="ps-148147"
export CLUSTER_NAME=$(/usr/share/google/get_metadata_value attributes/dataproc-cluster-name)
export INPUT_DIR="TestData"
export KAFKA_TOPIC="flights-data"
export BOOTSRTAP_SERVER="$CLUSTER_NAME-w-1:9092"