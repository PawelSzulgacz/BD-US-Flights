## Sprawozdanie Zestaw danych 6,Platforma Flink
# Producent - skrypty incjujące i zasilający

1. Uruchom środowisko na platformie Google Cloud
```
gcloud dataproc clusters create ${CLUSTER_NAME} \
--enable-component-gateway --region ${REGION} --subnet default \
--master-machine-type n1-standard-4 --master-boot-disk-size 50 \
--num-workers 2 --worker-machine-type n1-standard-2 --worker-boot-disk-size 50 \
--image-version 2.1-debian11 --optional-components ZOOKEEPER,DOCKER,FLINK \
--project ${PROJECT_ID} --max-age=3h \
--metadata "run-on-master=true" \
--initialization-actions gs://goog-dataproc-initialization-actions-${REGION}/kafka/kafka.sh
```
2. Uruchom 2 terminale
3. Pobierz dane (airports.csv, flights-2015.zip) i umieść w swoim bucketcie
4. Wrzuć plik .zip korzystając z terminalu
5. Rozpakuj plik i dodaj prawa
```
unzip projekt2.zip
chmod +x *.sh
```
6. Zmień plik env.sh żeby odpowiadał twoim danym
   - BUCKET_NAME - nazwa twojego bucketu
   - INPUT_DIR - folder z danymi typu event
   - STATIC_FILE - plik ze statycznymi danymi
7. Uruchom generator danych w terminalu nadawczym
```
./setup.sh
./produce.sh

```
# Utrzymanie obrazu rzeczywistego - transformacje
Dane otrzymywane z tematów kafki są deserializowane za pomocą klasy `FlightEventDeserializationSchema`
```java
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
```
Następnie dane są wzbogacane o stan, czas dostosowywany jest według stref czasowych, a także obliczane są opóźnienia w zależności od typu wydarzenia (start, lądowanie, odwołanie lotu)
```java
       DataStream<FlightLocEvent> enrichedFlightLocEventDS = FlightEventDS
                .map(new EnrichWithLocData(properties.get("AirportInput.uri")))
                .assignTimestampsAndWatermarks(
                WatermarkStrategy.<FlightLocEvent>forBoundedOutOfOrderness(Duration.ofSeconds(300)) // Maksymalne opóźnienie
                        .withTimestampAssigner((event, timestamp) -> event.getTimestamp())
        );
```
Dane statyczne odczytywane i mapowane są za pomocą funkcji `loadLocDataMap` w klasie `EnrichWithLocData`
```java
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
```
Dane z mapy są odczytywane w funkcji `map`
```java
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
```
Następnie używane do obliczeń (przykład dla zdarzenia oznaczającego przylot)
```java
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
```
Dane te następnie są agregowane i grupowane według stanu ameryki i dnia
```java
       DataStream<FlightStatsResult> FlightStatsDS = enrichedFlightLocEventDS
                .keyBy(new KeySelector<FlightLocEvent, Tuple2<String, String>>() {
                    @Override
                    public Tuple2<String, String> getKey(FlightLocEvent event) throws Exception {
                        return new Tuple2<>(event.getState(), event.getKeyDay());
                    }
                })
                .window(TumblingEventTimeWindows.of(Time.hours(24)))
                .trigger(EveryEventTimeTrigger.create())
                .aggregate(new FlightStatsAggregator(), new FlightStatsProcessWindowFunction());
```
`FlightStatsAggregator` Zawiera logikę, wykorzystywane podczas wyliczania statystyk dla każdej drużyny
```java
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
```
`FlightStatsProcessWindowFunction` Odpowiada za konwersję agregacji do obiektu `FlightStatsResult` przechowującego rezultaty agregacji.
```java
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
```
# Konsument: skrypt odczytujący wyniki przetwarzania
Uruchom przetwarzanie i odczytywanie wyników
```
./run.sh
```
Przykładowy wynik agregacji
```
5> FlightStatsResult{state='MA', day=04-01-2015, countDepartures=32, countArrivals=32, sumDelayDepartures=418, sumDelayArrivals=190}
5> FlightStatsResult{state='CO', day=04-01-2015, countDepartures=77, countArrivals=86, sumDelayDepartures=157, sumDelayArrivals=301}
5> FlightStatsResult{state='NY', day=04-01-2015, countDepartures=80, countArrivals=75, sumDelayDepartures=429, sumDelayArrivals=67}
5> FlightStatsResult{state='AZ', day=04-01-2015, countDepartures=54, countArrivals=45, sumDelayDepartures=400, sumDelayArrivals=212}
5> FlightStatsResult{state='MA', day=04-01-2015, countDepartures=33, countArrivals=32, sumDelayDepartures=418, sumDelayArrivals=190}
```
Pozostała część projektu nie została zaimplementowana.
