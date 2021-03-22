package model;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonUnwrapped;
import lombok.*;
import org.apache.log4j.Logger;
import util.transformers.TemperatureAggregationTransformer;

import java.util.Optional;

/**
 * Created by: Ian_Rakhmatullin
 * Date: 18.03.2021
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class HotelDailyData {
    private static final Logger log = Logger.getLogger(HotelDailyData.class);

    @JsonUnwrapped
    private Hotel hotel;
    private String date;
    private Double avg_tmpr_f;
    private Double avg_tmpr_c;

    @JsonIgnore
    @Getter(AccessLevel.NONE)
    private int count;

    private HotelDailyData(Builder builder) {
        hotel = builder.hotel;
        date = builder.date;
        avg_tmpr_c = builder.avg_tmpr_c;
        avg_tmpr_f = builder.avg_tmpr_f;
        count = builder.count;
    }

    /**
     * @return Combanation of the geoHash + weatherData. Used for joining with weather entities.
     */
    @JsonIgnore
    public String getHotelGeo2WeatherKey(){
        return hotel.getGeoHash() + "/" + date;
    }

    /**
     * @return Combanation of the id + weatherData. Used for stateful operations
     */
    @JsonIgnore
    public String getHotelId2WeatherKey(){
        return hotel.getId() + "/" + date;
    }

    public static HotelDailyData computeAverage(HotelDailyData data1, HotelDailyData data2) {
        boolean isFistWithTemperature = data1.isWithTemperature();
        boolean isSecondWithTemp = data2.isWithTemperature();

        Builder builder = newBuilder(data1);

        if (isSecondWithTemp){
            double avg_tmp_c_toSet;
            double avg_tmp_f_toSet;
            int countToSet;

            if (isFistWithTemperature){
                int count = data1.count;
                int count2 = data2.count;
                if (count2 > 1)
                    log.info("count2 " + count2);

                int newCount = count + 1;
                avg_tmp_c_toSet = ((data1.getAvg_tmpr_c() * count) + data2.getAvg_tmpr_c()) / newCount;
                avg_tmp_f_toSet = ((data1.getAvg_tmpr_f() * count) + data2.getAvg_tmpr_f()) / newCount;
                countToSet = newCount;
            }
            else{
                countToSet = 1;
                avg_tmp_c_toSet = data2.getAvg_tmpr_c();
                avg_tmp_f_toSet = data2.getAvg_tmpr_f();
            }

            builder.avg_tmpr_c = avg_tmp_c_toSet;
            builder.avg_tmpr_f = avg_tmp_f_toSet;
            builder.count = countToSet;

        }
        //todo
        log.info("Returning data1 with: hotel" + builder.hotel.getName() + "For the date: " + builder.date + " temperature is: " + builder.avg_tmpr_c);
        return builder.build();
    }


    /**
     * TODO
     * @return
     */
    @JsonIgnore
    public boolean isWithTemperature(){
        return this.getAvg_tmpr_c() != null;
    }


    public static Builder newBuilder(HotelDailyData copy) {
        Builder builder = new Builder();
        builder.hotel = copy.hotel;
        builder.date = copy.date;
        builder.avg_tmpr_c = copy.avg_tmpr_c;
        builder.avg_tmpr_c = copy.avg_tmpr_f;
        builder.count = copy.count;
        return builder;
    }


    public static final class Builder {
        private Hotel hotel;
        private String date;
        private Double avg_tmpr_c;
        private Double avg_tmpr_f;
        private int count;

        private Builder() {
        }

        public Builder setHotel(Hotel hotel) {
            this.hotel = hotel;
            return this;
        }

        public Builder setDate(String date) {
            this.date = date;
            return this;
        }

        public Builder setAvg_tmpr_c(Double avg_tmpr_c) {
            this.avg_tmpr_c = avg_tmpr_c;
            return this;
        }

        public Builder setAvg_tmpr_f(Double avg_tmpr_f) {
            this.avg_tmpr_f = avg_tmpr_f;
            return this;
        }

        public Builder setCount(int count) {
            this.count = count;
            return this;
        }

        public HotelDailyData build() {
            return new HotelDailyData(this);
        }
    }
}
