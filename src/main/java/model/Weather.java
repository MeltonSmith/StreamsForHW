package model;

import lombok.Data;

import java.util.Date;

/**
 * Created by: Ian_Rakhmatullin
 * Date: 17.03.2021
 */
@Data
public class Weather {
    private double latitude;
    private double longitude;
    private double avg_tmpr_f;
    private double avg_tmpr_c;
    private Date weatherDate;

}
