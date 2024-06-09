package com.example.bigdata.model;

import java.util.Date;
import com.example.bigdata.model.FlightStats;
public class FlightStatsAccumulator {
    private Long CountDepartures;
    private Long CountArrivals;
    private Long SumDelayDepartures;
    private Long SumDelayArrivals;


    public FlightStatsAccumulator() {
        this.CountDepartures = 0L;
        this.CountArrivals = 0L;
        this.SumDelayDepartures = 0L;
        this.SumDelayArrivals = 0L;
    }

    public void addDeparture(Long delay) {
        this.CountDepartures++;
        this.SumDelayDepartures += delay;
    }

    public void addArrival(Long delay) {
        this.CountArrivals++;
        this.SumDelayArrivals += delay;
    }

    public FlightStats getResult(){
        FlightStats results = new FlightStats();
        results.setCountDepartures(this.CountDepartures);
        results.setCountArrivals(this.CountArrivals);
        results.setSumDelayDepartures(this.SumDelayDepartures);
        results.setSumDelayArrivals(this.SumDelayArrivals);
        return results;
    }

    public FlightStatsAccumulator merge(FlightStatsAccumulator other) {
        this.CountDepartures += other.CountDepartures;
        this.CountArrivals += other.CountArrivals;
        this.SumDelayDepartures += other.SumDelayDepartures;
        this.SumDelayArrivals += other.SumDelayArrivals;
        return this;
    }
}
