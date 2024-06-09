source ./env.sh
flink run -m yarn-cluster -p 2\
   -yjm 1024m -ytm 1024m -c \
   com.example.bigdata.FlightDataAnalysis SensorDataAnalysis.jar \
   --AirportInput.uri $LOCAL_INPUT \
   --kafka.input.topic $KAFKA_TOPIC \
   --kafka.bootstrap.servers $BOOTSTRAP_SERVER \
   --kafka.group.id $KAFKA_GROUP_ID \
   $@