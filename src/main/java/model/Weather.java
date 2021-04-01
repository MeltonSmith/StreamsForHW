package model;


import ch.hsr.geohash.GeoHash;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.apache.log4j.Logger;

/**
 * Created by: Ian_Rakhmatullin
 * Date: 17.03.2021
 */

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class Weather {
    @JsonProperty("lat")
    private double latitude;
    @JsonProperty("lng")
    private double longitude;
    @JsonProperty("wthr_date")
    private String weatherDate;
    private double avg_tmpr_f;
    private double avg_tmpr_c;
    private String year;
    private String month;
    private String day;

    /**
     * @return GeoHash value for weather data rounded up to 4th character
     */
    private String getGeoHash(){
        return GeoHash.geoHashStringWithCharacterPrecision(latitude, longitude, 4);
    }

    /**
     * @return Combination of the geoHash + weatherData. Used for joining with hotel daily data entities.
     */
    @JsonIgnore
    public String getWeatherGeo2HotelKey(){
        return this.getGeoHash() + "/" + weatherDate;
    }
}
