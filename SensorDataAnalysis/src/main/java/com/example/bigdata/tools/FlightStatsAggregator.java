package com.example.bigdata.tools;
import org.apache.flink.api.common.functions.AggregateFunction;
import com.example.bigdata.model.FlightLocEvent;
import com.example.bigdata.model.FlightStats;
import com.example.bigdata.model.FlightStatsAccumulator;
public class FlightStatsAggregator implements AggregateFunction<FlightLocEvent, FlightStatsAccumulator, FlightStats>{
    @Override
    public FlightStatsAccumulator createAccumulator() {
        return new FlightStatsAccumulator();
    }

    @Override
    public FlightStatsAccumulator add(FlightLocEvent flightLocEvent, FlightStatsAccumulator flightStatsAccumulator) {
        Long Delay = Long.valueOf(flightLocEvent.getDelay());
        if (flightLocEvent.getType().equals("D")) {
            flightStatsAccumulator.addDeparture(Delay);
        } else if (flightLocEvent.getType().equals("A")) {
            flightStatsAccumulator.addArrival(Delay);
        }
        return flightStatsAccumulator;
    }

    @Override
    public FlightStats getResult(FlightStatsAccumulator flightStatsAccumulator) {
        return flightStatsAccumulator.getResult();
    }

    @Override
    public FlightStatsAccumulator merge(FlightStatsAccumulator a, FlightStatsAccumulator b) {
        return a.merge(b);
    }

}
