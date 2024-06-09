package com.example.bigdata.tools;
import com.example.bigdata.model.FlightStatsResult;
import com.example.bigdata.model.FlightEvent;
import com.example.bigdata.model.FlightStats;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import java.util.Date;
public class FlightStatsProcessWindowFunction extends ProcessWindowFunction<FlightStats, FlightStatsResult, Tuple2<String, String>, TimeWindow> {
    @Override
    public void process(Tuple2<String, String> key, Context context, Iterable<FlightStats> elements, Collector<FlightStatsResult> out) {
        FlightStats stats = elements.iterator().next();

        FlightStatsResult result = new FlightStatsResult(
            key.f0,
                key.f1,
            stats.getCountDepartures(),
            stats.getCountArrivals(),
            stats.getSumDelayDepartures(),
            stats.getSumDelayArrivals()
        );
        out.collect(result);
    }
}
