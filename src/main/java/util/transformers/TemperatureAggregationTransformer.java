package util.transformers;

import lombok.Data;
import model.HotelDailyData;
import org.apache.kafka.streams.kstream.ValueTransformer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.log4j.Logger;
import weatherStateStorage.WeatherStateStore;

/**
 * Created by: Ian_Rakhmatullin
 * Date: 18.03.2021
 */
@Data
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
        if (oldDailyData != null){
            //TODO cleanup
            Integer oldCount = countStore.get(key);
            int newCount = oldCount + 1;
            String name = hotelDailyData.getHotel().getName();
            log.info("Another join by date and geo is found...calculating average, new count is.." + newCount + " Hotel is " + name +". Date is: " + hotelDailyData.getDate());
            double newAvgValueC = ((oldDailyData.getAvg_tmpr_c() * oldCount) + hotelDailyData.getAvg_tmpr_c()) / newCount;
            double newAvgValueF = ((oldDailyData.getAvg_tmpr_f() * oldCount) + hotelDailyData.getAvg_tmpr_f()) / newCount;
            oldDailyData.setAvg_tmpr_c(newAvgValueC);
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
