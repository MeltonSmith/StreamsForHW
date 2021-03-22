package model;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonUnwrapped;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Created by: Ian_Rakhmatullin
 * Date: 18.03.2021
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class HotelDailyData {
    @JsonUnwrapped
    private Hotel hotel;
    private String date;
    private Double avg_tmpr_f;
    private Double avg_tmpr_c;

    /**
     * @return Combanation of the geoHash + weatherData. Used for joining with weather entities.
     */
    @JsonIgnore
    public String getHotel2WeatherKey(){
        return hotel.getGeoHash() + "/" + date;
    }
}
