package com.example.bigdata.model;

import java.util.Date;

public class FlightLocEvent {
    private String IATAStart;
    private String IATALand;
    private Date startTime;
    private Date landTime;
    private Date estimatedLandTime;
    private String KeyDay;
    private Integer Delay;
    private String Type;
    private String StateStart;
    private String StateLand;
    private Date Timestamp;

    public FlightLocEvent(String IATAStart, String IATALand, Date startTime, Date landTime, Date estimatedLandTime, String KeyDay, Integer delay, String type, String stateStart, String stateLand, Date Timestamp) {
        this.IATAStart = IATAStart;
        this.IATALand = IATALand;
        this.startTime = startTime;
        this.landTime = landTime;
        this.estimatedLandTime = estimatedLandTime;
        this.KeyDay = KeyDay;
        this.Delay = delay;
        this.Type = type;
        this.StateStart = stateStart;
        this.StateLand = stateLand;
        this.Timestamp = Timestamp;
    }

    public String getState(){
        if(getType().equals("D"))
        {
            return StateStart;
        }
        else if(getType().equals("A"))
        {
            return StateLand;
        }
        else{
            return "Unknown";
        }
    }

    public String getType() {
        return Type;
    }
    public String getKeyDay() {
        return KeyDay;
    }

    public Integer getDelay() {
        return Delay;
    }

    public String getStateStart() {
        return StateStart;
    }

    public String getStateLand() {
        return StateLand;
    }

    public Date getDate() {
        return startTime;
    }

    public Long getTimestamp() {
        return Timestamp.getTime();
    }

    @Override
    public String toString() {
        return "FlightLocEvent{" +
                "IATAStart='" + IATAStart + '\'' +
                ", IATALand='" + IATALand + '\'' +
                ", startTime=" + startTime +
                ", landTime=" + landTime +
                ", estimatedLandTime=" + estimatedLandTime +
                ", KeyDay=" + KeyDay +
                ", Delay=" + Delay +
                ", Type='" + Type + '\'' +
                ", StateStart='" + StateStart + '\'' +
                ", StateLand='" + StateLand + '\'' +
                ", Timestamp=" + Timestamp +
                '}';
    }
}
