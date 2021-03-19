package model;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;

/**
 * Created by: Ian_Rakhmatullin
 * Date: 18.03.2021
 */
@Data
@JsonFormat(with = JsonFormat.Feature.ACCEPT_CASE_INSENSITIVE_PROPERTIES)
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
    //rounded up to 4th symbol
    @JsonProperty("GeoHash")
    private String geoHash;
}
