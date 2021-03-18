package util.wrapper;

import ch.hsr.geohash.GeoHash;
import lombok.Data;

/**
 * Created by: Ian_Rakhmatullin
 * Date: 18.03.2021
 */
@Data
public class WeatherHotelKey {
    private GeoHash geoHash;
    private String weatherDate;
}
