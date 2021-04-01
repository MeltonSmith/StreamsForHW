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

import java.util.*;
import java.util.stream.Collectors;

import static org.junit.Assert.assertEquals;
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

    @Before
    public void setup() {
        final StreamsBuilder builder = WeatherHotelStream.getBuilder();

        testDriver = new TopologyTestDriver(builder.build(), getTestProperties());

        hotelsTopic = testDriver.createInputTopic(HOTELS_TOPIC, new StringSerializer(), new JsonSerializer<>());
        daysUniqueTopic = testDriver.createInputTopic(DAYS_UNIQUE, new StringSerializer(), new JsonSerializer<>());
        weatherTopic = testDriver.createInputTopic(WEATHER_RAW_TOPIC, new StringSerializer(), new JsonSerializer<>());

        outputTopic = testDriver.createOutputTopic(HOTEL_DAILY_DATA, new StringDeserializer(), new JsonDeserializer<>(HotelDailyData.class));
    }

    /**
     * Test case2 check the avg calculation
     */
    @Test
    public void calculatingAveragesCheck(){
        String firstDate = "2019-08-04";
        String secondDate = "2019-10-30";
        String outSideDate = "2019-12-30";

        Hotel firstHotel = createHotel(123, "NeroForte", "us", "Oklahoma", "1st Str", 49.2, -114.2);
        Hotel secondHotel = createHotel(1234, "BeforeIForget", "RU", "Moscow", "The Kremlin", 55.45, 37.37);
        Hotel thirdHotel = createHotel(12345, "SolwayFirth", "CA", "Ontario", "2nd Street", 50, 85);

        hotelsTopic.pipeInput(null, firstHotel);
        hotelsTopic.pipeInput(null, secondHotel);
        hotelsTopic.pipeInput(null, thirdHotel);

        daysUniqueTopic.pipeInput(DUMMY, createDateWrapper(firstDate));
        daysUniqueTopic.pipeInput(DUMMY, createDateWrapper(secondDate));

        //suitable coordinates:
        //1st hotel
        //1st Date avg should be equal to 12
        createWeatherData(firstDate, List.of(13d, 17d, 6d), firstHotel.getLatitude(), firstHotel.getLongitude());
        //2nd Date avg should be equal to 3
        createWeatherData(secondDate, List.of(2d, 4d), firstHotel.getLatitude(), firstHotel.getLongitude());

        //for 2nd Hotel
        //1st Date avg should be equal to 20
        createWeatherData(firstDate, List.of(10d, 20d, 10d, 10d, 5d, 5d, 10d), secondHotel.getLatitude(), secondHotel.getLongitude());
        //2nd Date avg should be equal to 5
        createWeatherData(secondDate, List.of(8d, 2d), secondHotel.getLatitude(), secondHotel.getLongitude());

        //outside data for confusing
        createWeatherData(outSideDate, Collections.singletonList(24d), 24, 138);


        Map<String, HotelDailyData> map = outputTopic.readKeyValuesToMap();

        //1st hotel result
        HotelDailyData firstHotelFirstDate = map.get(123 + "/" + firstDate);
        assertEquals(12, firstHotelFirstDate.getAvg_tmpr_c(),0.0);

        HotelDailyData firstHotelSecondDate = map.get(123 + "/" + secondDate);
        assertEquals( 3,firstHotelSecondDate.getAvg_tmpr_c(), 0.0);

        //2nd hotel result
        HotelDailyData secondHotelFirstDate = map.get(1234 + "/" + firstDate);
        assertEquals(10, secondHotelFirstDate.getAvg_tmpr_c(), 0.0);

        HotelDailyData secondHotelSecondDate = map.get(1234 + "/" + secondDate);
        assertEquals( 5, secondHotelSecondDate.getAvg_tmpr_c(),0.0);

        //3rd hotel SolwayFirth no weather data for him. Expecting 2 hotelDailyData with nulls instead of temperature values
        var collect = map.entrySet()
                        .stream()
                        .filter(entry -> entry.getKey().startsWith("12345/"))
                        .map(Map.Entry::getValue)
                        .collect(Collectors.toList());

        assertEquals(2,collect.size());

        var count = collect
                            .stream()
                            .filter(dailyData -> dailyData.getAvg_tmpr_c() != null)
                            .count();

        assertEquals(0, count);
    }

    /**
     * Output size and consistency test
     */
    @Test
    public void sizeAndConsistencyTest(){
        String firstDate = "2019-07-20";
        String secondDate = "2019-07-22";
        String thirdDate = "2020-08-24";

        Hotel firstHotel = createHotel(123, "CanYouHearMe", "us", "Oklahoma", "1st Str", 49.2, -114.2);
        Hotel secondHotel = createHotel(1234, "BecauseImLost", "RU", "Moscow", "The Kremlin", 55.45, 37.37);

        hotelsTopic.pipeInput(null, firstHotel);
        hotelsTopic.pipeInput(null, secondHotel);


        daysUniqueTopic.pipeInput(DUMMY, createDateWrapper(firstDate));
        daysUniqueTopic.pipeInput(DUMMY, createDateWrapper(secondDate));

        //suitable coordinates:
        //1st hotel
        createWeatherData(firstDate, Collections.singletonList(20d), firstHotel.getLatitude(), firstHotel.getLongitude());
        createWeatherData(secondDate, Collections.singletonList(21.4), firstHotel.getLatitude(), firstHotel.getLongitude());
        //2nd hotel
        createWeatherData(secondDate, Collections.singletonList(30d), secondHotel.getLatitude(), secondHotel.getLongitude());

        //no join coordinates, dates are ok.
        createWeatherData(firstDate, Collections.singletonList(21d), 50, -114.2);
        createWeatherData(secondDate, Collections.singletonList(24d), 31, 28);

        //appropriate coordinates but data does not exist in daysUnique. There should be no join with that record.
        createWeatherData(thirdDate, Collections.singletonList(31d), 49.2, -114.2);

        Map<String, HotelDailyData> map = outputTopic.readKeyValuesToMap();
        assertEquals(map.size(), 4);

        //1st hotel check
        HotelDailyData firstHotelFirstDate= map.get(firstHotel.getId() + "/" + firstDate); //should be with tmpr
        assertEquals(20, firstHotelFirstDate.getAvg_tmpr_c(), 0.0);

        HotelDailyData firstHotelSecondDate = map.get(firstHotel.getId() + "/" + secondDate); //should be with tmpr
        assertEquals(21.4, firstHotelSecondDate.getAvg_tmpr_c(), 0.0);

        //2nd hotel check
        HotelDailyData secondHotelFirstDate= map.get(secondHotel.getId() + "/" + firstDate); //no tmpr
        HotelDailyData secondHotelSecondDate = map.get(secondHotel.getId() + "/" + secondDate); //should be with tmpr
        Assert.assertNull(secondHotelFirstDate.getAvg_tmpr_c());
        assertEquals(30, secondHotelSecondDate.getAvg_tmpr_c(), 0.0);
    }

    @After
    public void tearDown() {
        testDriver.close();
    }

    /**
     * Utils.
     * Pipes into weatherTopic one or a few records, depending on temperature collection passed.
     *
     * @param date date for weather record
     * @param temperatures temperature range
     * @param latitude latitude of weather measurement
     * @param longitude longitude of weather measurement
     */
    private void createWeatherData(String date, Collection<Double> temperatures, double latitude, double longitude) {
        temperatures.forEach(temperature -> {
            weatherTopic.pipeInput(null, createWeather(date, temperature, latitude, longitude));
        });
    }

    /**
     * A Util method for creation of a hotel.
     */
    private Hotel createHotel(long id, String name, String country, String city, String address, double lat, double longitude){
        return Hotel.builder()
                        .id(id)
                        .name(name)
                        .country(country)
                        .city(city)
                        .address(address)
                        .latitude(lat)
                        .longitude(longitude)
                        .geoHash(GeoHash.geoHashStringWithCharacterPrecision(lat, longitude, 4))
                        .build();
    }

    /**
     * A Util method for creation of a dateWrapper.
     */
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
     * A Util method for creation of a weather.
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
     * Kafka streams test app config
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
