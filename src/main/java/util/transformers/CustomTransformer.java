package util.transformers;

import model.Weather;
import org.apache.kafka.streams.kstream.ValueTransformer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;
import util.wrapper.WeatherHotelKey;

/**
 * Created by: Ian_Rakhmatullin
 * Date: 18.03.2021
 */
public class CustomTransformer implements ValueTransformer<Weather, Weather>
{
    private KeyValueStore<WeatherHotelKey, String> stateStore;
    private ProcessorContext context;

    @Override
    public void init(ProcessorContext context) {
        this.context = context;
        //TODO find out what is store name
        this.stateStore = context.getStateStore("123");
    }

    @Override
    public Weather transform(Weather value) {
        return null;
    }

    @Override
    public void close() {

    }
}
