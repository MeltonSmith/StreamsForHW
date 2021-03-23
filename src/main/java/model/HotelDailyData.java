package model;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonUnwrapped;
import lombok.*;
import org.apache.log4j.Logger;


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
     * @return Combination of the geoHash + weatherData. Used for joining with weather entities.
     */
    @JsonIgnore
    public String getHotelGeo2WeatherKey(){
        return hotel.getGeoHash() + "/" + date;
    }

    /**
     * @return Combination of the id + weatherData. Used for stateful operations
     */
    @JsonIgnore
    public String getHotelId2WeatherKey(){
        return hotel.getId() + "/" + date;
    }

    /**
     * @return Defines whether this has temperature data.
     */
    @JsonIgnore
    public boolean isWithTemperature(){
        return this.getAvg_tmpr_c() != null;
    }



}
