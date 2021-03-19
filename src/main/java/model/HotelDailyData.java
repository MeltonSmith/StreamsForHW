package model;

import com.fasterxml.jackson.annotation.JsonUnwrapped;
import lombok.Data;
import util.wrapper.Weather2HotelKey;

/**
 * Created by: Ian_Rakhmatullin
 * Date: 18.03.2021
 */
@Data
public class HotelDailyData {
    @JsonUnwrapped
    private final Hotel hotel;
    private final String date;
    private Double avg_tmpr_f;
    private Double avg_tmpr_c;

    public Weather2HotelKey getWeather2HotelKey(){
        return new Weather2HotelKey(hotel.getGeoHash(), date);
    }
}
