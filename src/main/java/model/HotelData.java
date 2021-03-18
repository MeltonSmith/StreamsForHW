package model;

import lombok.Data;

/**
 * Created by: Ian_Rakhmatullin
 * Date: 18.03.2021
 */
@Data
public class HotelData {
    private int id;
    private String name;
    private String country;
    private String city;
    private String Address;
    private double latitude;
    private double longitude;
    //rounded up to 4th symbol
    private String geoHash;
}
