package model;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;

/**
 * Created by: Ian_Rakhmatullin
 * Date: 29.03.2021
 */
@Data
public class DayWrapper {
    @JsonProperty("wthr_date")
    private String formattedDate;
    private String year;
    private String month;
    private String day;
}
