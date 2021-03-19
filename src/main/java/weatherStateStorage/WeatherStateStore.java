package weatherStateStorage;

import joiners.Hotel2DateJoiner;
import model.Hotel;
import model.HotelDailyData;
import model.Weather;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.processor.WallclockTimestampExtractor;
import org.apache.kafka.streams.state.Stores;
import org.apache.log4j.Logger;
import util.serde.StreamSerdes;
import util.transformers.DeduplicateTransformer;
import util.transformers.TemperatureAggregationTransformer;

import java.time.Duration;
import java.util.Properties;

/**
 * Created by: Ian_Rakhmatullin
 * Date: 17.03.2021
 */
public class WeatherStateStore {
    public static final String WEATHER_RAW_TOPIC = "weather";
    public static final String WEATHER_UNIQUE_TOPIC = "weatherUnique";
    public static final String HOTELS_TOPIC = "hotels";
//    private final static Logger log = LoggerFactory.getLogger(WeatherStateStore.class);
    private static final Logger log = Logger.getLogger(WeatherStateStore.class);

    public static void main(String[] args) throws Exception{

//        TODO Подтянуть все даты каким-то образом в GlobalKTable (либо через Kafka топик, либо как-то еще);
//        TODO Добавить к отелям геохеш и развернуть стрим - по одной записи на каждую дату;
//        TODO Соединить с погодой по geohash + date;
//        TODO Сгруппировать по geo + date, для каждой группы посчитать avg(temp). Это будет stateful transformation;
//        TODO Результат выпустить в другой топик

//        Properties config = new Properties();
//        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "kafka-streams-app-id");
//        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9094");
//        config.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 100);
//        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
//        config.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 10);

//        StreamsConfig streamsConfig = new StreamsConfig(getProperties());
        Serde<String> stringSerde = Serdes.String();
        Serde<Weather> weatherSerde = StreamSerdes.weatherSerde();
        Serde<Hotel> hotelsSerde = StreamSerdes.hotelSerde();
        Serde<HotelDailyData> hotelDailyDataSerde = StreamSerdes.hotelDailyDataSerde();
//        JsonSerializer<Weather> purchase/**/JsonSerializer = new JsonSerializer<>();


        StreamsBuilder builder = new StreamsBuilder();

        // create store
        var storeBuilder = Stores.keyValueStoreBuilder(
                Stores.persistentKeyValueStore("dateStore"),
                Serdes.String(),
                Serdes.String());

        var dailyDataStore = Stores.keyValueStoreBuilder(
                Stores.persistentKeyValueStore("dailyDataStore"),
                Serdes.String(),
                StreamSerdes.hotelDailyDataSerde());

        var tempCountStore = Stores.keyValueStoreBuilder(
                Stores.persistentKeyValueStore("tempCountStore"),
                Serdes.String(),
                Serdes.Integer());

        // register store
        builder.addStateStore(storeBuilder);
        builder.addStateStore(dailyDataStore);
        builder.addStateStore(tempCountStore);

        ValueJoiner<Hotel, String, HotelDailyData> hotelDailyJoiner = new Hotel2DateJoiner();

        KStream<String, Weather> weatherRawStream = builder.stream(WEATHER_RAW_TOPIC, Consumed.with(stringSerde, weatherSerde));
        var uniqueDates = weatherRawStream
                                                .map((key, weather) -> KeyValue.pair("dummyKey", weather.getWeatherDate()))
                                                .transformValues(DeduplicateTransformer::new, "dateStore")
                                                .filter(((key, value) -> value != null))
                                                .peek((k, v) -> log.info("Date value " + v));

        JoinWindows twentyMinuteWindow =  JoinWindows.of(Duration.ofMinutes(20));

        //enriching the hotel data
       var hotelDailyStream = builder.stream(HOTELS_TOPIC,
                Consumed.with(stringSerde, hotelsSerde))
                .map((key, hotel) -> KeyValue.pair("dummyKey", hotel))
                .join(uniqueDates,
                        hotelDailyJoiner,
                        twentyMinuteWindow,
                        StreamJoined.with(stringSerde, hotelsSerde, stringSerde))
                .selectKey((k, v) -> v.getHotel2WeatherKey());

        KStream<String, Weather> weatherStreamKey = weatherRawStream
                                .selectKey((k, v) -> v.getWeather2HotelKey());

        hotelDailyStream
                .leftJoin(weatherStreamKey,
                        (hotelDailyData, weather) -> {
                            if (weather == null) //no join
                                return hotelDailyData;

                            //successful join
                            hotelDailyData.setAvg_tmpr_c(weather.getAvg_tmpr_c());
                            hotelDailyData.setAvg_tmpr_f(weather.getAvg_tmpr_f());
                            return hotelDailyData;
                        },
                        twentyMinuteWindow, StreamJoined.with(stringSerde, hotelDailyDataSerde, weatherSerde))
                .transformValues(TemperatureAggregationTransformer::new, "tempCountStore", "dailyDataStore")
                .peek((k, v) -> log.info("Date value of the final stream " + v));


//
//        builder.stream(HOTELS_TOPIC,
//                Consumed.with(stringSerde, hotelsSerde))
//                .map((key, hotel) -> KeyValue.pair("dummyKey", hotel))
//                .leftJoin(globalTable,
//                        (key, value) -> {
//                            return key;
//                        }, hotelDailyJoiner)
//                .peek((k, v) -> log.info("v " + v));




//        builder.stream(HOTELS_TOPIC,
//                Consumed.with(stringSerde, hotelsSerde) //key null
//                        .withOffsetResetPolicy(EARLIEST))
//                .join()
//                .transformValues(() -> new CustomTransformer())
//                .toStream()
//                .peek((k, v) -> log.info("Size " + v.size()));


//        KTable<String, HashMap<String, Long>> aggregate = topology.stream("input")
//                .groupBy((k, v) -> 0 /*map all records to same, arbitrary key*/)
//                .aggregate(() -> new HashMap<String, Long>(),
//                        (k, v, a) -> {
//                            Long count = a.get(v.get("state"));
//                            if (count == null) {
//                                count = 0L;
//                            }
//                            a.put(v.get("state"), ++count);
//                            return a;
//                        });


        KafkaStreams kafkaStreams = new KafkaStreams(builder.build(), getProperties());
        log.info("Started");
        kafkaStreams.cleanUp();
        kafkaStreams.start();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                kafkaStreams.close();
                log.info("Stream stopped");
            } catch (Exception exc) {
                log.error("Got exception while executing shutdown hook: ", exc);
            }
        }));

//        log.info("Started");
//        kafkaStreams.start();
//        Thread.sleep(120000);
//        log.info("Shutting down now");
//        kafkaStreams.close();
//        MockDataProducer.shutdown();


    }

    private static Properties getProperties() {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "weather-test");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "weather-aggregations-id");
        props.put(ConsumerConfig.CLIENT_ID_CONFIG, "weather-aggregations-client");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "30000");
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, "10000");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9094");
        props.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, "1");
        props.put(ConsumerConfig.METADATA_MAX_AGE_CONFIG, "10000");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.REPLICATION_FACTOR_CONFIG, 1);
        props.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, WallclockTimestampExtractor.class);
        return props;

    }
}
