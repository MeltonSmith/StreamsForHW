package joiners;

import model.Hotel;
import model.HotelDailyData;
import org.apache.kafka.streams.kstream.ValueJoiner;

/**
 * Created by: Ian_Rakhmatullin
 * Date: 19.03.2021
 */
public class Hotel2DateJoiner implements ValueJoiner<Hotel, String, HotelDailyData> {

    /**
     * @param hotel can't be null
     * @param date can't ne null
     * @return a hotelDailyData instance (a result from the left join from hotels to weather)
     */
    @Override
    public HotelDailyData apply(Hotel hotel, String date) {
        return new HotelDailyData(hotel, date, null, null, 0);
    }
}
