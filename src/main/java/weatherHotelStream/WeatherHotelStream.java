package weatherHotelStream;

import joiners.Hotel2DayJoiner;
import model.*;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.processor.WallclockTimestampExtractor;
import org.apache.log4j.Logger;
import util.serde.StreamSerdes;

import java.time.Duration;
import java.util.Properties;

/**
 * Created by: Ian_Rakhmatullin
 * Date: 17.03.2021
 */
public class WeatherHotelStream {
    private static final Logger log = Logger.getLogger(WeatherHotelStream.class);
    public static final String WEATHER_RAW_TOPIC = "weather";
    public static final String HOTELS_TOPIC = "hotels";
    public static final String HOTEL_DAILY_DATA = "hotelDailyData";
    public static final String DAYS_UNIQUE = "daysUnique";
    public static final String DUMMY = "dummy";

    public static void main(String[] args) throws Exception{
        StreamsBuilder builder = getBuilder();

        KafkaStreams kafkaStreams = new KafkaStreams(builder.build(), getProperties());
        log.info("Started");
        kafkaStreams.cleanUp();
        kafkaStreams.start();
        Thread.sleep(60000);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                kafkaStreams.close();
                log.info("Stream stopped");
            } catch (Exception exc) {
                log.error("Got exception while executing shutdown hook: ", exc);
            }
        }));
    }

    /**
     * Creates the needed topology
     */
    public static StreamsBuilder getBuilder() {
        Serde<String> stringSerde = Serdes.String();
        Serde<Weather> weatherSerde = StreamSerdes.weatherSerde();
        Serde<Hotel> hotelsSerde = StreamSerdes.hotelSerde();
        Serde<HotelDailyData> hotelDailyDataSerde = StreamSerdes.hotelDailyDataSerde();
        Serde<DayWrapper> daySerde = StreamSerdes.daySerde();

        StreamsBuilder builder = new StreamsBuilder();


        ValueJoiner<Hotel, DayWrapper, HotelDailyData> hotelDailyJoiner = new Hotel2DayJoiner();

        //unique dates taken from weather topic by hive, keys are already "dummy"
        var uniqueDates = builder.stream(DAYS_UNIQUE, Consumed.with(Serdes.String(), daySerde));

        JoinWindows window =  JoinWindows.of(Duration.ofDays(10));

        //enriching the hotel data with unique dates
        //making the key the combination of date+geoHash at the end
        var hotelDailyStream = builder.stream(HOTELS_TOPIC,
                Consumed.with(stringSerde, hotelsSerde))
                .map((key, hotel) -> KeyValue.pair(DUMMY, hotel))
                .join(uniqueDates,
                        hotelDailyJoiner,
                        window,
                        StreamJoined.with(stringSerde, hotelsSerde, daySerde))
                .selectKey((k, v) -> v.getHotelGeo2WeatherKey());
//                .peek((k, v) -> System.out.println("join with" + v.getHotel() + " date is " + v.getDate()));

        KStream<String, Weather> weatherRawStream = builder.stream(WEATHER_RAW_TOPIC, Consumed.with(stringSerde, weatherSerde));
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
                             }
                             return hotelDailyData;
                         },
                         window, StreamJoined.with(stringSerde, hotelDailyDataSerde, weatherSerde))
                 .groupBy((k, v) -> v.getHotelId2WeatherKey(), Grouped.with(Serdes.String(), hotelDailyDataSerde))
                 .aggregate(HotelDailyDataAggregator::new,
                         (key, value, aggregator) -> {
                             //init aggregator
                             if (aggregator.getHotelDailyData() == null)
                                 aggregator.setHotelDailyData(value);
                             //new value has temperature - recalculating the average value
                             if (value.isWithTemperature()) {
                                 aggregator.recalculateAvg(value);
                             }
                             return aggregator;
                         },
                         Materialized.with(Serdes.String(), StreamSerdes.hotelDailyDataAggregatorSerdeSerde()))
                .mapValues(HotelDailyDataAggregator::getHotelDailyData, Materialized.with(Serdes.String(), hotelDailyDataSerde))
                .toStream()
                .to(HOTEL_DAILY_DATA, Produced.with(Serdes.String(), hotelDailyDataSerde));

        return builder;
    }

    /**
     * Kafka streams app config
     */
    public static Properties getProperties() {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "weather");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, 400000);
        props.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 400000);
        props.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 8);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9094");
        props.put(ConsumerConfig.METADATA_MAX_AGE_CONFIG, "10000");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.REPLICATION_FACTOR_CONFIG, 1);
        props.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, WallclockTimestampExtractor.class);
        return props;
    }
}
