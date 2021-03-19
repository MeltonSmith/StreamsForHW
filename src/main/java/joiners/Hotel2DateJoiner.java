package joiners;

import model.Hotel;
import model.HotelDailyData;
import org.apache.kafka.streams.kstream.ValueJoiner;

/**
 * Created by: Ian_Rakhmatullin
 * Date: 19.03.2021
 */
public class Hotel2DateJoiner implements ValueJoiner<Hotel, String, HotelDailyData> {

    @Override
    public HotelDailyData apply(Hotel hotel, String s) {
        return null;
    }
}
