package com.example.bigdata.model;

import java.util.Date;

public class FlightStatsResult extends FlightStats{
    private String state;
    private String day;

    public FlightStatsResult(String state, String day, Long countDepartures, Long countArrivals, Long sumDelayDepartures, Long sumDelayArrivals) {
        super(countDepartures, countArrivals, sumDelayDepartures, sumDelayArrivals);
        this.state = state;
        this.day = day;
    }


    public String getState() {
        return state;
    }
    public String getDay() {
        return day;
    }

    @Override
    public String toString() {
        return "FlightStatsResult{" +
                "state='" + getState() + '\'' +
                ", day=" + getDay() +
                ", countDepartures=" + getCountDepartures() +
                ", countArrivals=" + getCountArrivals() +
                ", sumDelayDepartures=" + getSumDelayDepartures() +
                ", sumDelayArrivals=" + getSumDelayArrivals() +
                '}';
    }

}
