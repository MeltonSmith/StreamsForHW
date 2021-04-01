import ch.hsr.geohash.GeoHash;
import model.DayWrapper;
import model.Hotel;
import model.HotelDailyData;
import model.Weather;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.processor.WallclockTimestampExtractor;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import util.serde.JsonDeserializer;
import util.serde.JsonSerializer;
import weatherHotelStream.WeatherHotelStream;

import java.util.Properties;

import static weatherHotelStream.WeatherHotelStream.*;

/**
 * Created by: Ian_Rakhmatullin
 * Date: 31.03.2021
 */
public class WeatherHotelStreamTest {

    private TopologyTestDriver testDriver;
    private TestInputTopic<String, Hotel> hotelsTopic;
    private TestInputTopic<String, DayWrapper> daysUniqueTopic;
    private TestInputTopic<String, Weather> weatherTopic;
    private TestOutputTopic<String, HotelDailyData> outputTopic;

//    private final Instant recordBaseTime = Instant.parse("2019-06-01T10:00:00Z");
//    private final Duration advance1Min = Duration.ofMinutes(1);

    @Before
    public void setup() {
        final StreamsBuilder builder = WeatherHotelStream.getBuilder();

        //Create Actual Stream Processing pipeline
//        KafkaStreams kafkaStreams = new KafkaStreams(builder.build(), WeatherHotelStream.getProperties());
        testDriver = new TopologyTestDriver(builder.build(), getTestProperties());

        hotelsTopic = testDriver.createInputTopic(HOTELS_TOPIC, new StringSerializer(), new JsonSerializer<>());
        daysUniqueTopic = testDriver.createInputTopic(DAYS_UNIQUE, new StringSerializer(), new JsonSerializer<>());
        weatherTopic = testDriver.createInputTopic(WEATHER_RAW_TOPIC, new StringSerializer(), new JsonSerializer<>());

        outputTopic = testDriver.createOutputTopic(HOTEL_DAILY_DATA, new StringDeserializer(), new JsonDeserializer<>(HotelDailyData.class));
    }

    @Test
    public void test1(){
        hotelsTopic.pipeInput(null, createHotel(123, "HotelName", "us", "Oklahoma", "1st Str", 49.2d, -114.2d));
        daysUniqueTopic.pipeInput("dummy", createDateWrapper("2019-07-20"));
        daysUniqueTopic.pipeInput("dummy", createDateWrapper("2019-07-21"));
        weatherTopic.pipeInput(null, createWeather("2019-07-20", 20d, 49.2d, -114.2d));

        Assert.assertEquals(2, outputTopic.getQueueSize());
        Assert.assertFalse(outputTopic.isEmpty());
        HotelDailyData hotelDailyData = outputTopic.readValue();
        Assert.assertEquals(hotelDailyData.getDate(), "2019-07-20");
    }

    @After
    public void tearDown() {
        testDriver.close();
    }

    private Hotel createHotel(long id, String name, String country, String city, String address, double lat, double longitude){
        return Hotel.builder()
                        .id(id)
                        .name(name)
                        .country(country)
                        .city(city)
                        .address(address)
                        .latitude(lat)
                        .latitude(longitude)
                        .geoHash(GeoHash.geoHashStringWithCharacterPrecision(lat, longitude, 4))
                        .build();
    }

    private DayWrapper createDateWrapper(String date){
        String[] split = date.split("-");

        return DayWrapper.builder()
                .formattedDate(date)
                .day(split[2])
                .month(split[1])
                .year(split[0])
                .build();
    }

    /**
     * TODO
     * @param date
     * @param avg_tmpr_c
     * @param latitude
     * @param longitude
     * @return
     */
    private Weather createWeather(String date, double avg_tmpr_c, double latitude, double longitude){
        String[] split = date.split("-");

        return
                Weather.builder()
                        .weatherDate(date)
                        .year(split[0])
                        .month(split[1])
                        .day(split[2])
                        .avg_tmpr_c(avg_tmpr_c)
                        .avg_tmpr_f((avg_tmpr_c / 0.5556) + 32)
                        .latitude(latitude)
                        .longitude(longitude)
                        .build();

    }


    /**
     * Kafka streams app config
     */
    public static Properties getTestProperties() {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "weather-test");
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
