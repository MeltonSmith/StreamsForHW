package joiners;

import model.DayWrapper;
import model.Hotel;
import model.HotelDailyData;
import org.apache.kafka.streams.kstream.ValueJoiner;

/**
 * Created by: Ian_Rakhmatullin
 * Date: 19.03.2021
 */
public class Hotel2DayJoiner implements ValueJoiner<Hotel, DayWrapper, HotelDailyData> {

    /**
     * This method creates an instance of the HotelDailyData class, for combination of hotel + date.
     * Only date is taken from the weather parameter, since we only want to enrich hotel data with unique dates
     *
     * @param hotel can't be null since this joiner and constructor is used in "cross join" with dummy keys
     * @param dayWrapper can't ne null since this joiner and constructor is used in "cross join" with dummy keys
     * @return a hotelDailyData instance (a result from the cross join from hotels to weather)
     */
    @Override
    public HotelDailyData apply(Hotel hotel, DayWrapper dayWrapper) {
        return new HotelDailyData(hotel, dayWrapper);
    }
}
