package util.transformers;

import lombok.Data;
import model.HotelDailyData;
import org.apache.kafka.streams.kstream.ValueTransformer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;
import util.wrapper.Weather2HotelKey;

/**
 * Created by: Ian_Rakhmatullin
 * Date: 18.03.2021
 */
@Data
public class CustomTransformer implements ValueTransformer<HotelDailyData, HotelDailyData>
{
    private KeyValueStore<Weather2HotelKey, HotelDailyData> dailyDataStore;
    private KeyValueStore<Weather2HotelKey, Integer> countStore;

    private ProcessorContext context;

    @Override
    public void init(ProcessorContext context) {
        this.context = context;
        //TODO find out what is store name
        this.dailyDataStore = context.getStateStore("123");
        this.countStore = context.getStateStore("1423");
    }

    @Override
    public HotelDailyData transform(HotelDailyData hotelDailyData) {
        var key = hotelDailyData.getWeather2HotelKey();
        var oldDailyData = dailyDataStore.get(key);
        if (oldDailyData != null){
            //TODO cleanup
            Integer currentCount = countStore.get(key);
            ++currentCount;
            double newAvgValueC = ((oldDailyData.getAvg_tmpr_c() * currentCount) + hotelDailyData.getAvg_tmpr_c()) / currentCount;
            double newAvgValueF = ((oldDailyData.getAvg_tmpr_f() * currentCount) + hotelDailyData.getAvg_tmpr_f()) / currentCount;
            oldDailyData.setAvg_tmpr_c(newAvgValueC);
            oldDailyData.setAvg_tmpr_f(newAvgValueF);
            countStore.put(key, currentCount);
            return oldDailyData;
        } else{
          countStore.put(key, 1);
          return hotelDailyData;
        }
    }

    @Override
    public void close() {

    }
}
