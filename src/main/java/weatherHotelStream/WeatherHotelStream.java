package weatherHotelStream;

import joiners.Hotel2DateJoiner;
import model.Hotel;
import model.HotelDailyData;
import model.HotelDailyDataAggregator;
import model.Weather;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.processor.WallclockTimestampExtractor;
import org.apache.kafka.streams.state.*;
import org.apache.log4j.Logger;
import util.serde.StreamSerdes;
import util.transformers.DeduplicateTransformer;

import java.time.Duration;
import java.util.Properties;
import java.util.function.Consumer;

/**
 * Created by: Ian_Rakhmatullin
 * Date: 17.03.2021
 */
public class WeatherHotelStream {
    public static final String WEATHER_RAW_TOPIC = "weather";
    public static final String HOTELS_TOPIC = "hotels";
    private static final Logger log = Logger.getLogger(WeatherHotelStream.class);
    public static final String DAILY_DATA_STORE = "dailyDataStore";
    public static final String TEMP_COUNT_STORE = "tempCountStore";

    public static void main(String[] args) throws Exception{
        Serde<String> stringSerde = Serdes.String();
        Serde<Weather> weatherSerde = StreamSerdes.weatherSerde();
        Serde<Hotel> hotelsSerde = StreamSerdes.hotelSerde();
        Serde<HotelDailyData> hotelDailyDataSerde = StreamSerdes.hotelDailyDataSerde();

        StreamsBuilder builder = new StreamsBuilder();

        //creating stores:
        //for unique dates
        var storeBuilder = Stores.keyValueStoreBuilder(
                Stores.persistentKeyValueStore("dateStore"),
                Serdes.String(),
                Serdes.String());

//        var average1 = Stores.keyValueStoreBuilder(
//                Stores.persistentKeyValueStore("average"),
//                Serdes.String(),
//                StreamSerdes.hotelDailyDataSerde());

        //for unique hotel+day combination
        var dailyDataStore = Stores.keyValueStoreBuilder(
                Stores.persistentKeyValueStore(DAILY_DATA_STORE),
                Serdes.String(),
                StreamSerdes.hotelDailyDataSerde());


        //for calculating average values
        var tempCountStore = Stores.keyValueStoreBuilder(
                Stores.persistentKeyValueStore(TEMP_COUNT_STORE),
                Serdes.String(),
                Serdes.Integer());

        var testStore = Stores.keyValueStoreBuilder(
                Stores.persistentKeyValueStore("test"),
                Serdes.String(),
                StreamSerdes.hotelDailyDataSerde());



        // registering stores
        builder.addStateStore(storeBuilder);
        builder.addStateStore(dailyDataStore);
        builder.addStateStore(tempCountStore);

        ValueJoiner<Hotel, String, HotelDailyData> hotelDailyJoiner = new Hotel2DateJoiner();

        KStream<String, Weather> weatherRawStream = builder.stream(WEATHER_RAW_TOPIC, Consumed.with(stringSerde, weatherSerde));
        var uniqueDates = getUniqueDates(weatherRawStream);

        JoinWindows twentyMinuteWindow =  JoinWindows.of(Duration.ofMinutes(20));

        //enriching the hotel data with unique dates
        //making the key the combination of date+geoHash at the end
        var hotelDailyStream = builder.stream(HOTELS_TOPIC,
                Consumed.with(stringSerde, hotelsSerde))
                .map((key, hotel) -> KeyValue.pair("dummyKey", hotel))
                .join(uniqueDates,
                        hotelDailyJoiner,
                        twentyMinuteWindow,
                        StreamJoined.with(stringSerde, hotelsSerde, stringSerde))
                .selectKey((k, v) -> v.getHotelGeo2WeatherKey());

        //changing the keys from "dummy" to combination of date+geoHash
        var weatherStreamKey = weatherRawStream
                .selectKey((k, v) -> v.getWeatherGeo2HotelKey());

        //trying to join hotels to weather (left)
       hotelDailyStream
                .leftJoin(weatherStreamKey,
                        (hotelDailyData, weather) -> {
                            if (weather != null) {
                                hotelDailyData.setAvg_tmpr_c(weather.getAvg_tmpr_c());
                                hotelDailyData.setAvg_tmpr_f(weather.getAvg_tmpr_f());
                                hotelDailyData.setCount(1);
                            }
                            return hotelDailyData;
                        },
                        twentyMinuteWindow, StreamJoined.with(stringSerde, hotelDailyDataSerde, weatherSerde))
                .groupBy((k, v) -> v.getHotelId2WeatherKey(), Grouped.with(Serdes.String(), StreamSerdes.hotelDailyDataSerde()))
                .aggregate(HotelDailyDataAggregator::new,
                        (key, value, aggregator) -> {
                            //init
                            if (aggregator.getHotelDailyData() == null)
                                aggregator.setHotelDailyData(value);
                            //new value has temperature - recalculating the average value
                            if (value.isWithTemperature()) {
                                aggregator.recalculateAvg(value);
                            }
                            return aggregator;
                        },
                        Materialized.with(Serdes.String(), StreamSerdes.hotelDailyDataAggregatorSerdeSerde()))
                .mapValues(HotelDailyDataAggregator::getHotelDailyData, Materialized.<String, HotelDailyData, KeyValueStore<Bytes, byte[]>>as("test").withKeySerde(Serdes.String()).withValueSerde(StreamSerdes.hotelDailyDataSerde()));
//                .toStream()
//                .to("hotelDailyData", Produced.with(Serdes.String(), StreamSerdes.hotelDailyDataSerde()));



        KafkaStreams kafkaStreams = new KafkaStreams(builder.build(), getProperties());
        log.info("Started");
        kafkaStreams.cleanUp();
        kafkaStreams.start();

        Thread.sleep(60000);

        ReadOnlyKeyValueStore<String, HotelDailyData> store = kafkaStreams.store("test", QueryableStoreTypes.keyValueStore());
        KeyValueIterator<String, HotelDailyData> iterator = store.all();
        Producer<String, HotelDailyData> producer = new KafkaProducer<>(getProperties());
        while (iterator.hasNext()) {
            KeyValue<String, HotelDailyData> next = iterator.next();
            send(producer, next.key, next.value);
        }

        log.info("Closing Kafka Producer");
        producer.close();

        log.info("closed");
        kafkaStreams.close();

//
//        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
//            try {
//                kafkaStreams.close();
//
//                ReadOnlyKeyValueStore<String, HotelDailyData> store = kafkaStreams.store("test", QueryableStoreTypes.keyValueStore());
//                KeyValueIterator<String, HotelDailyData> iterator = store.all();
//                while (iterator.hasNext()) {
//                    KeyValue<String, HotelDailyData> next = iterator.next();
//
//                }
//                Producer<String, String> producer = new KafkaProducer<>(getProperties());
//
//                store.get("1");
//                kafkaStreams.close();
//                log.info("Stream stopped");
//            } catch (Exception exc) {
//                log.error("Got exception while executing shutdown hook: ", exc);
//            }
//        }));
    }

    private static void send(Producer<String, HotelDailyData> producer, String key, HotelDailyData hotelDailyData) {
        ProducerRecord<String, HotelDailyData> record = new ProducerRecord<>("hotelDailyDataUnique", key, hotelDailyData);
        producer.send(record);
    }

    /**
     * The main purpose - to deduplicate dates in the whole stream of weather data, taken from "weather" topic in Kafka
     * @param weatherRawStream - raw weather data as it is in kafka
     * @return stream of only unique dates. Key is dummy for enriching hotels with date by a join.
     */
    private static KStream<String, String> getUniqueDates(KStream<String, Weather> weatherRawStream) {
        return weatherRawStream
                .map((key, weather) -> KeyValue.pair("dummyKey", weather.getWeatherDate()))
                .transformValues(DeduplicateTransformer::new, "dateStore")
                .filter(((key, value) -> value != null))
                .peek((k, v) -> log.info("Date value " + v));
    }

    private static Properties getProperties() {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "weather-test");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "weather-aggregations-id");
        props.put(ConsumerConfig.CLIENT_ID_CONFIG, "weather-aggregations-client");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "30000");
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, "10000");
        props.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, "1");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9094");
        props.put(ConsumerConfig.METADATA_MAX_AGE_CONFIG, "10000");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.REPLICATION_FACTOR_CONFIG, 1);
        props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, "0");
        props.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, WallclockTimestampExtractor.class);
        return props;

    }
}
