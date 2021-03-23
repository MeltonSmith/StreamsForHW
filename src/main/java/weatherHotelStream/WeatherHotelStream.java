package weatherHotelStream;

import joiners.Hotel2DateJoiner;
import model.Hotel;
import model.HotelDailyData;
import model.HotelDailyDataAggregator;
import model.Weather;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.processor.ThreadMetadata;
import org.apache.kafka.streams.processor.WallclockTimestampExtractor;
import org.apache.kafka.streams.state.*;
import org.apache.log4j.Logger;
import util.serde.JsonSerializer;
import util.serde.StreamSerdes;
import util.transformers.DeduplicateTransformer;

import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.function.Consumer;
import java.util.stream.Collectors;

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
    public static final String HOTEL_DAILY_DATA_UNIQUE = "hotelDailyDataUnique";

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

        //store for the final result
        var finalData = Stores.keyValueStoreBuilder(
                Stores.persistentKeyValueStore("finalData"),
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
                .mapValues(HotelDailyDataAggregator::getHotelDailyData, Materialized.<String, HotelDailyData, KeyValueStore<Bytes, byte[]>>as("finalData").withKeySerde(Serdes.String()).withValueSerde(StreamSerdes.hotelDailyDataSerde()));



        KafkaStreams kafkaStreams = new KafkaStreams(builder.build(), getProperties());
        log.info("Started");
        kafkaStreams.cleanUp();
        kafkaStreams.start();
        Thread.sleep(60000);

        //trying to write the current state of the "finalData" store to make the records unique per key(hotelId+data)
        Metric metric = kafkaStreams.metrics()
                .entrySet()
                .stream()
                .filter(metricNameEntry -> metricNameEntry.getKey().name().equals("process-rate"))
                .filter(metricNameEntry -> metricNameEntry.getKey().group().equals("stream-thread-metrics"))
                .map(Map.Entry::getValue)
                .findFirst().orElse(null);

        while(true){
            assert metric != null;
            if (((Double) metric.metricValue()) == 0.0d){
                log.info("active tasks is zero, writing a final topic for hotelDailyData");
                //taking a stateStore for final KTable
                ReadOnlyKeyValueStore<String, HotelDailyData> store = kafkaStreams.store("finalData", QueryableStoreTypes.keyValueStore());
                KeyValueIterator<String, HotelDailyData> iterator = store.all();
                Producer<String, HotelDailyData> producer = new KafkaProducer<>(getPropertiesForProducer());
                while (iterator.hasNext()) {
                    KeyValue<String, HotelDailyData> next = iterator.next();
                    send(producer, next.key, next.value);
                }
                log.info("Closing Kafka Producer");
                producer.close();

                log.info("Closing streams");
                kafkaStreams.close();
                break;
            }
        }

//
//        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
//            try {
//                kafkaStreams.close();
//                log.info("Stream stopped");
//            } catch (Exception exc) {
//                log.error("Got exception while executing shutdown hook: ", exc);
//            }
//        }));
    }

    private static void send(Producer<String, HotelDailyData> producer, String key, HotelDailyData hotelDailyData) {
        ProducerRecord<String, HotelDailyData> record = new ProducerRecord<>(HOTEL_DAILY_DATA_UNIQUE, key, hotelDailyData);
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

    /**
     * Producer API config
     */
    private static Properties getPropertiesForProducer(){
        Properties properties = new Properties();
        properties.put("bootstrap.servers", "localhost:9094");
        properties.put("acks", "all");
        properties.put("retries", 0);
        properties.put("batch.size", 16384);
        properties.put("linger.ms", 1);
        properties.put("buffer.memory", 33554432);

        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);

        return properties;
    }

    /**
     * Kafka streams app config
     */
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
