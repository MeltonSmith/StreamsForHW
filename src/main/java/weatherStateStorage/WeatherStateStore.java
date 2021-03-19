package weatherStateStorage;

import joiners.Hotel2DateJoiner;
import model.Hotel;
import model.HotelDailyData;
import model.Weather;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.processor.WallclockTimestampExtractor;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;
import org.apache.log4j.Logger;
import util.serde.StreamSerdes;
import util.transformers.CustomTransformer;
import util.transformers.DeduplicateTransformer;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Properties;

import static org.apache.kafka.streams.Topology.AutoOffsetReset.EARLIEST;
import static org.apache.kafka.streams.Topology.AutoOffsetReset.LATEST;

/**
 * Created by: Ian_Rakhmatullin
 * Date: 17.03.2021
 */
public class WeatherStateStore {
    public static final String WEATHER_RAW_TOPIC = "weather";
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

        //TODO джойнит похоже что только по ключам
        //

        StreamsBuilder builder = new StreamsBuilder();

        // create store
        StoreBuilder storeBuilder = Stores.keyValueStoreBuilder(
                Stores.persistentKeyValueStore("dateStore"),
                Serdes.String(),
                Serdes.String());
        // register store
        builder.addStateStore(storeBuilder);

        ValueJoiner<Hotel, String, HotelDailyData> hotelDailyJoiner = new Hotel2DateJoiner();

        KTable<String, String> dateKTable = builder.stream(WEATHER_RAW_TOPIC,
                Consumed.with(stringSerde, weatherSerde))
                    .mapValues(Weather::getWeatherDate)
                    .transformValues(DeduplicateTransformer::new, "dateStore")
                    .filter(((key, value) -> value != null))
                    .peek((k, v) -> log.info("Date value " + v))
                    .toTable();

        builder.stream(HOTELS_TOPIC,
                Consumed.with(stringSerde, hotelsSerde))
                .join(dateKTable,
                        hotelDailyJoiner,
                        Joined.with(stringSerde, hotelsSerde, stringSerde))
                .peek((k, v) -> log.info("v " + v));




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
//        MockDataProducer.produceStockTransactions(15, 50, 25, false);
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
