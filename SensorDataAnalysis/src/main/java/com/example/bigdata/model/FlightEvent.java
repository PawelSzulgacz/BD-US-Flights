package com.example.bigdata.model;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
public class FlightEvent {
    private String startAirport;
    private String destAirport;
    private Date scheduledDepartureTime;
    private Date scheduledArrivalTime;
    private Date departureTime;
    private Integer ScheduledFlightTime;
    private Integer taxiOut;
    private Integer taxiIn;
    private Date arrivalTime;
    private Date orderColumn;
    private String infoType;


    private static final String[] DATE_FORMATS = {
            "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'",
            "yyyy-MM-dd'T'HH:mm:ss'Z'",
            "yyyy-MM-dd HH:mm:ss",
            "yyyy-MM-dd",
            ""
    };

    private static Date parseDate(String dateStr) throws ParseException {
        if (dateStr.equals("\"\"")){
            return null;
        }
        for (String format : DATE_FORMATS) {
            try {
                return new SimpleDateFormat(format).parse(dateStr);
            } catch (ParseException e) {

            }
        }
        throw new ParseException("Unparseable date: " + dateStr, 0);
    }

    private static int parseInt(String intStr) {
        if (intStr.equals("\"\"")) {
            return 0;
        }
        return Integer.parseInt(intStr);
    }

    public void setStartAirport(String startAirport) {
        this.startAirport = startAirport;
    }
    public void setDestAirport(String destAirport) {
        this.destAirport = destAirport;
    }
    public void setScheduledDepartureTime(Date scheduledDepartureTime) {
        this.scheduledDepartureTime = scheduledDepartureTime;
    }
    public void setScheduledArrivalTime(Date scheduledArrivalTime) {
        this.scheduledArrivalTime = scheduledArrivalTime;
    }
    public void setDepartureTime(Date departureTime) {
        this.departureTime = departureTime;
    }
    public void setScheduledFlightTime(Integer ScheduledFlightTime) {
        this.ScheduledFlightTime = ScheduledFlightTime;
    }
    public void setTaxiOut(Integer taxiOut) {
        this.taxiOut = taxiOut;
    }
    public void setTaxiIn(Integer taxiIn) {
        this.taxiIn = taxiIn;
    }
    public void setArrivalTime(Date arrivalTime) {
        this.arrivalTime = arrivalTime;
    }
    public void setOrderColumn(Date orderColumn) {
        this.orderColumn = orderColumn;
    }
    public void setInfoType(String infoType) {
        this.infoType = infoType;
    }



    public String getStartAirport() {
        return startAirport;
    }
    public String getDestAirport() {
        return destAirport;
    }
    public String getInfoType() {
        return infoType;
    }
    public Date getScheduledDepartureTime() {
        return scheduledDepartureTime;
    }
    public Date getScheduledArrivalTime() {
        return scheduledArrivalTime;
    }
    public Date getDepartureTime() {
        return departureTime;
    }
    public Integer getScheduledFlightTime() {
        return ScheduledFlightTime;
    }
    public Integer getTaxiOut() {
        return taxiOut;
    }
    public Integer getTaxiIn() {
        return taxiIn;
    }
    public Date getArrivalTime() {
        return arrivalTime;
    }
    public Long getTimestamp() {
        return orderColumn.getTime();
    }

    public Date getOrderColumn() {
        return orderColumn;
    }


    public static FlightEvent fromString(String line) throws ParseException {
        String[] parts = line.split(",");
        if (parts.length == 25) {
            FlightEvent FlightEvent = new FlightEvent();
            FlightEvent.startAirport = parts[3];
            FlightEvent.destAirport = parts[4];
            FlightEvent.scheduledDepartureTime = parseDate(parts[5]);
            FlightEvent.ScheduledFlightTime = parseInt(parts[7]);
            FlightEvent.scheduledArrivalTime = parseDate(parts[8]);
            FlightEvent.departureTime = parseDate(parts[9]);
            FlightEvent.taxiOut = parseInt(parts[10]);
            FlightEvent.taxiIn = parseInt(parts[12]);
            FlightEvent.arrivalTime = parseDate(parts[13]);
            FlightEvent.orderColumn = parseDate(parts[23]);
            FlightEvent.infoType = parts[24];;
            return FlightEvent;
        } else {
            throw new IllegalArgumentException("Malformed line: " + parts[3] + "," + parts[4] + "," + parts[5] + "," + parts[9] + "," + parts[12] + "," + parts[13] + "," + parts[23] + "," + parts[24]);
        }
    }

    @Override
    public String toString() {
        return "FlightEvent{" +
                "startAirport='" + startAirport + '\'' +
                ", destAirport='" + destAirport + '\'' +
                ", scheduledDepartureTime=" + scheduledDepartureTime +
                ", scheduledArrivalTime=" + scheduledArrivalTime +
                ", departureTime=" + departureTime +
                ", ScheduledFlightTime=" + ScheduledFlightTime +
                ", taxiOut=" + taxiOut +
                ", taxiIn=" + taxiIn +
                ", arrivalTime=" + arrivalTime +
                ", orderColumn=" + orderColumn +
                ", infoType='" + infoType + '\'' +
                '}';
    }
}








