package com.example.bigdata.connectors;

import com.example.bigdata.model.FlightEvent;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.source.reader.deserializer.KafkaRecordDeserializationSchema;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Date;

public class FlightEventDeserializationSchema implements KafkaRecordDeserializationSchema<FlightEvent> {

    private DateTimeFormatter formatter;
    private static final String[] DATE_FORMATS = {
            "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'",
            "yyyy-MM-dd'T'HH:mm:ss'Z'",
            "yyyy-MM-dd HH:mm:ss",
            "yyyy-MM-dd",
            ""
    };

    private static Date parseDate(String dateStr){
        if (dateStr.equals("\"\"")){
            return null;
        }
        for (String format : DATE_FORMATS) {
            try {
                return new SimpleDateFormat(format).parse(dateStr);
            } catch (ParseException e) {
                throw new RuntimeException(e);
            }
        }
        return null;
    }

    private static int parseInt(String intStr) {
        if (intStr.equals("\"\"")) {
            return 0;
        }
        return Integer.parseInt(intStr);
    }

    @Override
    public void open(DeserializationSchema.InitializationContext context) {
        formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
    }

    @Override
    public void deserialize(ConsumerRecord<byte[], byte[]> record, Collector<FlightEvent> out) {
        String[] parts = new String(record.value()).split(",");
        FlightEvent event = new FlightEvent();
        event.setStartAirport(parts[3]);
        event.setDestAirport(parts[4]);
        event.setScheduledDepartureTime(parseDate(parts[5]));
        event.setScheduledArrivalTime(parseDate(parts[6]));
        event.setDepartureTime(parseDate(parts[7]));
        event.setScheduledFlightTime(parseInt(parts[8]));
        event.setTaxiOut(parseInt(parts[9]));
        event.setTaxiIn(parseInt(parts[10]));
        event.setArrivalTime(parseDate(parts[11]));
        out.collect(event);
    }


    @Override
    public TypeInformation<FlightEvent> getProducedType() {
        return TypeInformation.of(FlightEvent.class);
    }
}