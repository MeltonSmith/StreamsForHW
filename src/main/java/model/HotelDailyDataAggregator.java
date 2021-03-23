package model;

import com.fasterxml.jackson.annotation.JsonUnwrapped;
import lombok.Data;
import org.apache.log4j.Logger;

import java.util.Optional;

/**
 * Class-aggregator for calculating averag temperatures of hotel daily data
 *
 * Created by: Ian_Rakhmatullin
 * Date: 23.03.2021
 */
@Data
public class HotelDailyDataAggregator {
    private static final Logger log = Logger.getLogger(HotelDailyDataAggregator.class);

    @JsonUnwrapped
    private HotelDailyData hotelDailyData;
    private int currentCount;

    /**
     * Recalculates avg C and avg F for a particular hotelDailyData nested in this aggregator
     * @param value incoming record
     */
    public void recalculateAvg(HotelDailyData value){
        int newCount = currentCount + 1;
        var currentAvgC = Optional.ofNullable(getHotelDailyData().getAvg_tmpr_c());
        currentAvgC.ifPresentOrElse(a -> hotelDailyData.setAvg_tmpr_c(((a * currentCount) + value.getAvg_tmpr_c()) / newCount), () -> hotelDailyData.setAvg_tmpr_c(value.getAvg_tmpr_c()));

        var currentAvgF = Optional.ofNullable(getHotelDailyData().getAvg_tmpr_f());
        currentAvgF.ifPresentOrElse(a -> hotelDailyData.setAvg_tmpr_f(((a * currentCount) + value.getAvg_tmpr_f()) / newCount), () -> hotelDailyData.setAvg_tmpr_f(value.getAvg_tmpr_f()));

        this.currentCount = newCount;

        log.info("Recalculating HotelDaily: " + hotelDailyData.getHotel().getName() + " new count: " + currentCount + ". For the date: " + hotelDailyData.getDate() + " new avg is: " + hotelDailyData.getAvg_tmpr_c());
    }
}
