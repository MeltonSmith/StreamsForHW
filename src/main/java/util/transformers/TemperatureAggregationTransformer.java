package util.transformers;

import model.HotelDailyData;
import org.apache.kafka.streams.kstream.ValueTransformer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.log4j.Logger;

/**
 *
 * Calculates averages temperatures for a particular day for a hotel.
 *
 * Created by: Ian_Rakhmatullin
 * Date: 18.03.2021
 */
public class TemperatureAggregationTransformer implements ValueTransformer<HotelDailyData, HotelDailyData>
{
    private static final Logger log = Logger.getLogger(TemperatureAggregationTransformer.class);
    private KeyValueStore<String, HotelDailyData> dailyDataStore;
    private KeyValueStore<String, Integer> countStore;

    private ProcessorContext context;

    @Override
    public void init(ProcessorContext context) {
        this.context = context;
        this.dailyDataStore = context.getStateStore("dailyDataStore");
        this.countStore = context.getStateStore("tempCountStore");
    }

    @Override
    public HotelDailyData transform(HotelDailyData hotelDailyData) {
        var key = hotelDailyData.getHotel2WeatherKey();
        var oldDailyData = dailyDataStore.get(key);
        return getOrRefreshDailyData(hotelDailyData, key, oldDailyData);
    }

    /**
     *
     * @param hotelDailyData current hotelDailyData is being processed
     * @param key by which hotelDailyData entities and counts are saved in stores.
     * @param oldDailyData already existing entity for this hotel+day. (If any)
     * @return refreshed hotelDailyData entity
     */
    private HotelDailyData getOrRefreshDailyData(HotelDailyData hotelDailyData, String key, HotelDailyData oldDailyData) {
        if (oldDailyData != null){
            Integer oldCount = countStore.get(key);
            int newCount = oldCount + 1;

            //calculating new average values for both temperatures
            double newAvgValueC = ((oldDailyData.getAvg_tmpr_c() * oldCount) + hotelDailyData.getAvg_tmpr_c()) / newCount;
            oldDailyData.setAvg_tmpr_c(newAvgValueC);
            double newAvgValueF = ((oldDailyData.getAvg_tmpr_f() * oldCount) + hotelDailyData.getAvg_tmpr_f()) / newCount;
            oldDailyData.setAvg_tmpr_f(newAvgValueF);

            countStore.put(key, newCount);
            return null;
        } else{
            countStore.put(key, 1);
            dailyDataStore.put(key, hotelDailyData);
            return hotelDailyData;
        }
    }

    @Override
    public void close() {

    }
}
