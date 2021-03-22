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


//        boolean withTemperature = data1.isWithTemperature();
//        boolean isSecondWithTemp = data2.isWithTemperature();
//
//        if (isSecondWithTemp){
//            double avg_tmp_c_toSet;
//            double avg_tmp_f_toSet;
//
//            if (withTemperature){
//                int count = data1.count;
//                int newCount = count + 1;
//                avg_tmp_c_toSet = ((data1.getAvg_tmpr_c() * count) + data2.getAvg_tmpr_c())/newCount;
//                avg_tmp_f_toSet = ((data1.getAvg_tmpr_f() * count) + data2.getAvg_tmpr_f())/newCount;
//                data1.count = newCount;
//            }
//            else{
//                data1.count = 1;
//                avg_tmp_c_toSet = data2.getAvg_tmpr_c();
//                avg_tmp_f_toSet = data2.getAvg_tmpr_f();
//            }
//            data1.setAvg_tmpr_c(avg_tmp_c_toSet);
//            data1.setAvg_tmpr_f(avg_tmp_f_toSet);
//        }
//        log.info("Returning data1 with: hotel" + data1.getHotel().getName() + "For the date: " + data1.getDate() + " temperature is: " + data1.getAvg_tmpr_c());
//        return data1;
        return data1;
    }


    /**
     * TODO
     * @return
     */
    @JsonIgnore
    public boolean isWithTemperature(){
        return this.getAvg_tmpr_c() != null;
    }
}
