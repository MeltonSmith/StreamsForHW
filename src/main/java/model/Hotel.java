package model;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Created by: Ian_Rakhmatullin
 * Date: 18.03.2021
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class Hotel {

    @JsonProperty("Id")
    private long id;
    @JsonProperty("Name")
    private String name;
    @JsonProperty("Country")
    private String country;
    @JsonProperty("City")
    private String city;
    @JsonProperty("Address")
    private String address;
    @JsonProperty("Latitude")
    private double latitude;
    @JsonProperty("Longitude")
    private double longitude;
    @JsonProperty("GeoHash")
    private String geoHash;
}
