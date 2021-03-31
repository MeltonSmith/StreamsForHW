package model;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonUnwrapped;
import lombok.*;
import org.apache.log4j.Logger;


/**
 * Created by: Ian_Rakhmatullin
 * Date: 18.03.2021
 */
@Data
@NoArgsConstructor
public class HotelDailyData {
    private static final Logger log = Logger.getLogger(HotelDailyData.class);

    @JsonUnwrapped
    private Hotel hotel;
    private Double avg_tmpr_f;
    private Double avg_tmpr_c;
    @JsonProperty("wthr_date")
    private String date;
    private String year;
    private String month;
    private String day;

    /**
     * Only date is taken from the weather parameter, since we only want to enrich hotel data with unique dates via cross join.
     */
    public HotelDailyData(Hotel hotel, DayWrapper dayWrapper){
        this.hotel = hotel;
        this.date = dayWrapper.getFormattedDate();
        this.year = dayWrapper.getYear();
        this.month = dayWrapper.getMonth();
        this.day = dayWrapper.getDay();
    }

    /**
     * @return Combination of the geoHash + weatherData. Used for joining with weather entities.
     */
    @JsonIgnore
    public String getHotelGeo2WeatherKey(){
        return hotel.getGeoHash() + "/" + date;
    }

    /**
     * @return Combination of the id + weatherData. Used for stateful operations.
     */
    @JsonIgnore
    public String getHotelId2WeatherKey(){
        return hotel.getId() + "/" + date;
    }

    /**
     * @return Defines whether this daily data has temperature.
     */
    @JsonIgnore
    public boolean isWithTemperature(){
        return this.getAvg_tmpr_c() != null;
    }
}
