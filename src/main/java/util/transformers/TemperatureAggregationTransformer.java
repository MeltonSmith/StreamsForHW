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
@Deprecated
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
        var key = hotelDailyData.getHotelId2WeatherKey();
        var oldDailyData = dailyDataStore.get(key);
        if (oldDailyData!=null && !oldDailyData.getHotel().getName().equals(hotelDailyData.getHotel().getName()))
            throw new IllegalStateException("Collision with the key: " + key);
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
            //calculating new average values for both temperatures
            mergeTemperature(hotelDailyData, key, oldDailyData);
            return null; //discard the record
        } else{
            dailyDataStore.put(key, hotelDailyData); //new entry
            mergeTemperature(hotelDailyData, key, null);
            return hotelDailyData;
        }
    }

    /**
     * Merges the temperature for a hotel per day.
     * @param hotelDailyData current hotelDailyData is being processed
     * @param key by which hotelDailyData entities and counts are saved in stores.
     * @param oldDailyData already existing entity for this hotel+day. (If any)
     */
    private void mergeTemperature(HotelDailyData hotelDailyData, String key, HotelDailyData oldDailyData) {
        if (!hotelDailyData.isWithTemperature())
            return; //if current record has no temperature (unsuccessful join) there is no need to merge

        if (oldDailyData == null) { //adding the first entry with a such key and this entry has a temperature after the join
            countStore.put(key, 1);
            return;
        }

        //if we have some old data
        Integer oldCount = countStore.get(key);

        if (oldCount == null) { //old data was with no temperature (replace)
            oldDailyData.setAvg_tmpr_c(hotelDailyData.getAvg_tmpr_c());
            oldDailyData.setAvg_tmpr_f(hotelDailyData.getAvg_tmpr_f());
            dailyDataStore.put(key, oldDailyData);
            countStore.put(key, 1);
        } else { //old data and new data with temperature (calculating average)
            int newCount = oldCount + 1;

            String name = hotelDailyData.getHotel().getName();
            log.info("Another join by date and geo is found...calculating average, new count is.." + newCount + " Hotel is " + name +". Date is: " + hotelDailyData.getDate());
            double newAvgValueC = ((oldDailyData.getAvg_tmpr_c() * oldCount) + hotelDailyData.getAvg_tmpr_c()) / newCount;
            oldDailyData.setAvg_tmpr_c(newAvgValueC);
            double newAvgValueF = ((oldDailyData.getAvg_tmpr_f() * oldCount) + hotelDailyData.getAvg_tmpr_f()) / newCount;
            oldDailyData.setAvg_tmpr_f(newAvgValueF);
            dailyDataStore.put(key, oldDailyData);
            countStore.put(key, newCount);
        }
    }

    @Override
    public void close() {

    }
}
