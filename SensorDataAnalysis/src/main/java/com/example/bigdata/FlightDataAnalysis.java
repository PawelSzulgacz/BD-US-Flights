package com.example.bigdata;

import com.example.bigdata.connectors.Connectors;
import com.example.bigdata.model.*;
import com.example.bigdata.tools.EnrichWithLocData;
import com.example.bigdata.tools.FlightStatsAggregator;
import com.example.bigdata.tools.FlightStatsProcessWindowFunction;
import com.example.bigdata.windows.EveryEventTimeTrigger;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.EventTimeTrigger;

import java.time.Duration;

public class FlightDataAnalysis {
    public static void main(String[] args) throws Exception {

        //ParameterTool propertiesFromFile = ParameterTool.fromPropertiesFile("src/main/resources/flink.properties");
        //ParameterTool propertiesFromArgs = ParameterTool.fromArgs(args);
        //ParameterTool properties = propertiesFromFile.mergeWith(propertiesFromArgs);
        ParameterTool properties = ParameterTool.fromArgs(args);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.getConfig().setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 10000));
        env.enableCheckpointing(10000, CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().setCheckpointTimeout(60000);

        DataStreamSource<FlightEvent> FlightEventDS = env.fromSource(Connectors.getFlightSource(properties), WatermarkStrategy.noWatermarks(), "Flight Source");

        /*DataStream<String> inputStream = env.
               fromSource(Connectors.getFileSource(properties),
                        WatermarkStrategy.noWatermarks(), "FlightData");


        DataStream<FlightEvent> FlightEventDS = inputStream
                .filter(new FilterFunction<String>() {
                    @Override
                    public boolean filter(String line) {
                        return !line.startsWith("airline");
                    }
                })
                .map(new MapFunction<String, FlightEvent>() {
                    @Override
                    public FlightEvent map(String txt) throws Exception {
                        return FlightEvent.fromString(txt);
                    }
                })
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<FlightEvent>forBoundedOutOfOrderness(Duration.ofSeconds(300)) // Maksymalne opóźnienie
                                .withTimestampAssigner((event, timestamp) -> event.getTimestamp())
                );
        */

        DataStream<FlightLocEvent> enrichedFlightLocEventDS = FlightEventDS
                .map(new EnrichWithLocData(properties.get("AirportInput.uri")))
                .assignTimestampsAndWatermarks(
                WatermarkStrategy.<FlightLocEvent>forBoundedOutOfOrderness(Duration.ofSeconds(300)) // Maksymalne opóźnienie
                        .withTimestampAssigner((event, timestamp) -> event.getTimestamp())
        );

        DataStream<FlightStatsResult> FlightStatsDS = enrichedFlightLocEventDS
                .keyBy(new KeySelector<FlightLocEvent, Tuple2<String, String>>() {
                    @Override
                    public Tuple2<String, String> getKey(FlightLocEvent event) throws Exception {
                        return new Tuple2<>(event.getState(), event.getKeyDay());
                    }
                })// Assuming getDate() returns a formatted date string
                .window(TumblingEventTimeWindows.of(Time.hours(24)))
                .trigger(EveryEventTimeTrigger.create())
                .aggregate(new FlightStatsAggregator(), new FlightStatsProcessWindowFunction());

        FlightStatsDS.print();
        //enrichedFlightLocEventDS.print();
        env.execute("FlightDataAnalysis");
    }
}
