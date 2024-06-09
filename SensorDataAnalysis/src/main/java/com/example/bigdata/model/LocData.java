package com.example.bigdata.model;

public class LocData {
    private String IATA;
    private Integer Timezone;
    private String State;

    public LocData(String IATA, Integer timezone, String state) {
        this.IATA = IATA;
        this.Timezone = timezone;
        this.State = state;

    }

    // Konstruktor bezparametrowy
    public LocData() {
        // Domyślne inicjalizacje, można dostosować do potrzeb
        this.IATA = "";
        this.Timezone = -100;
        this.State = "";
    }
    public String getIATA() {
        return IATA;
    }
    public Integer getTimezone() {
        return Timezone;
    }
    public String getState() {
        return State;
    }


    @Override
    public String toString() {
        return "LocData{" +
                "IATA='" + IATA + '\'' +
                ", Timezone=" + Timezone +
                ", State='" + State + '\'' +
                '}';
    }
}
