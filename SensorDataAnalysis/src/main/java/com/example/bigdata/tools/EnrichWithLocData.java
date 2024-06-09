package com.example.bigdata.tools;

import com.example.bigdata.model.LocData;
import com.example.bigdata.model.FlightEvent;
import com.example.bigdata.model.FlightLocEvent;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Date;

public class EnrichWithLocData extends RichMapFunction<FlightEvent, FlightLocEvent> {
    private final String locFilePath;
    private Map<String, LocData> locDataMap;

    public EnrichWithLocData(String locFilePath) {
        this.locFilePath = locFilePath;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        locDataMap = loadLocDataMap();
    }

    private Date addTimezone(Date date, Integer timezone) {
        Calendar cal = Calendar.getInstance();
        cal.setTime(date);
        cal.add(Calendar.HOUR, timezone);
        return cal.getTime();
    }

    private Date addMinutes(Date date, Integer minutes) {
        Calendar cal = Calendar.getInstance();
        cal.setTime(date);
        cal.add(Calendar.MINUTE, minutes);
        return cal.getTime();
    }

    @Override
    public FlightLocEvent map(FlightEvent FlightEvent) throws Exception {
        SimpleDateFormat dayFormat = new SimpleDateFormat("dd-MM-yyyy");
        String type = FlightEvent.getInfoType();
        String IATAStart = FlightEvent.getStartAirport();
        String IATALand = FlightEvent.getDestAirport();
        LocData locDataStart = locDataMap.get(IATAStart);
        LocData locDataLand = locDataMap.get(IATALand);
        String stateStart = (locDataStart != null) ? locDataStart.getState() : "Unknown";
        String stateLand = (locDataLand != null) ? locDataLand.getState() : "Unknown";
        Integer TimezoneStart = (locDataStart != null) ? locDataStart.getTimezone() : Integer.valueOf(0);
        Integer TimezoneLand = (locDataLand != null) ? locDataLand.getTimezone() : Integer.valueOf(0);
        if(type.equals("D"))
        {
            //estlandtime - wylot + czas lotu + taxiout
            Date estLandTime = addTimezone(FlightEvent.getDepartureTime(), TimezoneStart);
            estLandTime = addMinutes(estLandTime, FlightEvent.getScheduledFlightTime() + FlightEvent.getTaxiOut());
            //delay = estlandtime - landtime
            Date scheduledDepartueTime = addTimezone(FlightEvent.getScheduledDepartureTime(), TimezoneStart);
            Date departureTime = addTimezone(FlightEvent.getDepartureTime(), TimezoneStart);
            Integer delay = Math.toIntExact((scheduledDepartueTime.getTime() - departureTime.getTime()) / (60 * 1000)) + FlightEvent.getTaxiOut();
            delay = Math.max(delay, 0);
            String KeyDay = dayFormat.format(departureTime);
            return new FlightLocEvent(
                IATAStart,
                IATALand,
                departureTime,
                addTimezone(FlightEvent.getScheduledArrivalTime(), TimezoneLand),
                estLandTime,
                KeyDay,
                delay,
                FlightEvent.getInfoType(),
                stateStart,
                stateLand,
                FlightEvent.getOrderColumn()
            );
        }
        else if(type.equals("A"))
        {
            //delay - arrival time + taxiIn
            Date arrivalTime = FlightEvent.getArrivalTime();
            if(arrivalTime == null)
            {
                arrivalTime = FlightEvent.getScheduledArrivalTime();
            }
            arrivalTime = addTimezone(arrivalTime, TimezoneLand);
            Date departureTime = addTimezone(FlightEvent.getDepartureTime(), TimezoneStart);
            Date scheduledArrivalTime = FlightEvent.getScheduledArrivalTime();
            scheduledArrivalTime = addTimezone(scheduledArrivalTime, TimezoneLand);
            Integer delay = Math.toIntExact((scheduledArrivalTime.getTime() - arrivalTime.getTime()) / (60 * 1000)) + FlightEvent.getTaxiIn();
            delay = Math.max(delay, 0);
            String KeyDay = dayFormat.format(arrivalTime);
            return new FlightLocEvent(
                    IATAStart,
                    IATALand,
                    departureTime,
                    arrivalTime,
                    arrivalTime,
                    KeyDay,
                    delay,
                    FlightEvent.getInfoType(),
                    stateStart,
                    stateLand,
                    FlightEvent.getOrderColumn()
            );
        }
        else
        {
            return new FlightLocEvent(
                    "Unknown",
                    "Unknown",
                    new Date(),
                    new Date(),
                    new Date(),
                    "",
                    0,
                    type,
                    "Unknown",
                    "Unknown",
                    FlightEvent.getOrderColumn()
            );
        }

    }

    public Map<String, LocData> loadLocDataMap() throws IOException {
        Map<String, LocData> map = new HashMap<>();

        try (BufferedReader reader = new BufferedReader(new FileReader(locFilePath))) {
            String line;
            boolean headerSkipped = false;

            while ((line = reader.readLine()) != null) {
                if (!headerSkipped) {
                    headerSkipped = true;
                    continue;
                }
                String[] parts = line.split(",");
                if (parts.length == 14) {
                    String IATA = parts[4];
                    Integer Timezone = Integer.parseInt(parts[9]);
                    String State = parts[13];
                    LocData locData = new LocData(IATA, Timezone, State);
                    map.put(IATA, locData);
                }
            }
        }
        return map;
    }
}

