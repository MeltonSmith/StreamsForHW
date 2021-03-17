package weatherStateStorage;

import model.Weather;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.connect.json.JsonSerializer;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.processor.WallclockTimestampExtractor;
import org.apache.kafka.streams.state.StoreBuilder;
import org.apache.kafka.streams.state.Stores;

import java.util.Date;
import java.util.Properties;

import static org.apache.kafka.streams.Topology.AutoOffsetReset.EARLIEST;

/**
 * Created by: Ian_Rakhmatullin
 * Date: 17.03.2021
 */
public class WeatherStateStore {
    public static final String WEATHER_RAW_TOPIC = "weather";


    public static void main(String[] args) {

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

        StreamsConfig streamsConfig = new StreamsConfig(getProperties());
        Serde<String> stringSerde = Serdes.String();

        new JSonPOj
        JsonSerializer<Weather> purchaseJsonSerializer = new JsonSerializer<>();


        StreamsBuilder builder = new StreamsBuilder();

        KTable<String, Date> shareVolume = builder.stream(WEATHER_RAW_TOPIC,
                Consumed.with(stringSerde, Serdes.)
                        .withOffsetResetPolicy(EARLIEST))
                .mapValues(st -> ShareVolume.newBuilder(st).build())
                .groupBy((k, v) -> v.getSymbol(), Serialized.with(stringSerde, shareVolumeSerde))
                .reduce(ShareVolume::sum);



    }

    private static Properties getProperties() {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "weather-aggregations");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "weather-aggregations-id");
        props.put(ConsumerConfig.CLIENT_ID_CONFIG, "weather-aggregations-client");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
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
