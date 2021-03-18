package util.wrapper;


import lombok.Data;
import lombok.RequiredArgsConstructor;

/**
 * Created by: Ian_Rakhmatullin
 * Date: 18.03.2021
 */
@Data
@RequiredArgsConstructor
public class Weather2HotelKey {
    private final String geoHash;
    private final String weatherDate;
}
