package com.example.bigdata.connectors;

import com.example.bigdata.model.FlightEvent;
import com.example.bigdata.connectors.FlightEventDeserializationSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.TextLineInputFormat;
import org.apache.flink.core.fs.Path;

import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import java.time.Duration;
public class Connectors {
    public static FileSource<String> getFileSource(ParameterTool properties) {
        return FileSource
                .forRecordStreamFormat(new TextLineInputFormat(),
                        new Path(properties.getRequired("fileInput.uri")))
                .monitorContinuously(Duration.ofMillis(
                        Long.parseLong(properties.getRequired("fileInput.interval"))))
                .build();
    }

    public static KafkaSource<FlightEvent> getFlightSource(ParameterTool properties) {
        return KafkaSource.<FlightEvent>builder()
                .setBootstrapServers(properties.getRequired("kafka.bootstrap-servers"))
                .setTopics(properties.getRequired("kafka.input-topic"))
                .setGroupId(properties.getRequired("kafka.group.id"))
                .setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.EARLIEST))
                .setDeserializer(new FlightEventDeserializationSchema())
                .build();
    }


}
