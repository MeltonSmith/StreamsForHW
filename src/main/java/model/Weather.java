package model;


import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;

/**
 * Created by: Ian_Rakhmatullin
 * Date: 17.03.2021
 */

@Data
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
}
