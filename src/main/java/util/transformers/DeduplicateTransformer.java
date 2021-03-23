package util.transformers;

import model.Weather;
import org.apache.kafka.streams.kstream.ValueTransformer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;

/**
 * Discards duplicates for weather entities.
 *
 * Created by: Ian_Rakhmatullin
 * Date: 19.03.2021
 */
public class DeduplicateTransformer implements ValueTransformer<Weather, Weather> {

    private KeyValueStore<String, Weather> dateStore;
    private ProcessorContext context;
    @Override
    public void init(ProcessorContext context) {
        this.context = context;
        this.dateStore = context.getStateStore("dateStore");
    }

    @Override
    public Weather transform(Weather value) {
        Weather output;
        String key = value.getWeatherDate();
        if (dateStore.get(key) != null) {
            output = null; //discard the record
        } else {
            output = value;
            dateStore.put(key, value);
        }
        return output;
    }

    @Override
    public void close() {

    }
}
