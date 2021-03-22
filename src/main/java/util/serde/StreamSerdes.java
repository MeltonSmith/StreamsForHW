package util.serde;

import model.Hotel;
import model.HotelDailyData;
import model.Weather;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

/**
 * Created by: Ian_Rakhmatullin
 * Date: 18.03.2021
 */
public class StreamSerdes {

    public static Serde<Weather> weatherSerde() {
        return new WeatherSerde();
    }

    public static Serde<Hotel> hotelSerde() {
        return new HotelSerde();
    }

    public static Serde<HotelDailyData> hotelDailyDataSerde() {
        return new HotelDailyDataSerde();
    }

    private static final class HotelDailyDataSerde extends WrapperSerde<HotelDailyData> {
        public HotelDailyDataSerde() {
            super(new JsonSerializer<>(), new JsonDeserializer<>(HotelDailyData.class));
        }
    }

    private static final class WeatherSerde extends WrapperSerde<Weather> {
        public WeatherSerde() {
            super(new JsonSerializer<>(), new JsonDeserializer<>(Weather.class));
        }
    }

    private static final class HotelSerde extends WrapperSerde<Hotel> {
        public HotelSerde() {
            super(new JsonSerializer<>(), new JsonDeserializer<>(Hotel.class));
        }
    }

    public abstract static class WrapperSerde<T> implements Serde<T> {
        private final JsonSerializer<T> serializer;
        private final JsonDeserializer<T> deserializer;

        protected WrapperSerde(JsonSerializer<T> serializer, JsonDeserializer<T> deserializer) {
            this.serializer = serializer;
            this.deserializer = deserializer;
        }

        @Override
        public Serializer<T> serializer() {
            return serializer;
        }

        @Override
        public Deserializer<T> deserializer() {
            return deserializer;
        }
    }
}
