package model;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Created by: Ian_Rakhmatullin
 * Date: 29.03.2021
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class DayWrapper {
    @JsonProperty("wthr_date")
    private String formattedDate;
    private String year;
    private String month;
    private String day;
}
