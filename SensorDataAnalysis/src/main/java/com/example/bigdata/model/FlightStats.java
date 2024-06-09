package com.example.bigdata.model;

import java.util.Date;

public class FlightStats {

    private Long CountDepartures;
    private Long CountArrivals;
    private Long SumDelayDepartures;
    private Long SumDelayArrivals;


    public FlightStats() {

        this.CountDepartures = 0L;
        this.CountArrivals = 0L;
        this.SumDelayDepartures = 0L;
        this.SumDelayArrivals = 0L;
    }
    public FlightStats( Long countDepartures, Long countArrivals, Long sumDelayDepartures, Long sumDelayArrivals) {

        this.CountDepartures = countDepartures;
        this.CountArrivals = countArrivals;
        this.SumDelayDepartures = sumDelayDepartures;
        this.SumDelayArrivals = sumDelayArrivals;
    }



    public void setCountDepartures(Long countDepartures) {
        this.CountDepartures = countDepartures;
    }

    public void setCountArrivals(Long countArrivals) {
        this.CountArrivals = countArrivals;
    }

    public void setSumDelayDepartures(Long sumDelayDepartures) {
        this.SumDelayDepartures = sumDelayDepartures;
    }

    public void setSumDelayArrivals(Long sumDelayArrivals) {
        this.SumDelayArrivals = sumDelayArrivals;
    }


    public Long getCountDepartures() {
        return CountDepartures;
    }
    public Long getCountArrivals() {
        return CountArrivals;
    }
    public Long getSumDelayDepartures() {
        return SumDelayDepartures;
    }
    public Long getSumDelayArrivals() {
        return SumDelayArrivals;
    }




    @Override
    public String toString() {
        return "FlightStats{" +
                ", CountDepartures=" + CountDepartures +
                ", CountArrivals=" + CountArrivals +
                ", SumDelayDepartures=" + SumDelayDepartures +
                ", SumDelayArrivals=" + SumDelayArrivals +
                '}';
    }

}
