package util.transformers;

import org.apache.kafka.streams.kstream.ValueTransformer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.state.KeyValueStore;

/**
 * Discards duplicates for weather entities.
 *
 * Created by: Ian_Rakhmatullin
 * Date: 19.03.2021
 */
public class DeduplicateTransformer implements ValueTransformer<String, String> {

    private KeyValueStore<String, String> dateStore;
    private ProcessorContext context;
    @Override
    public void init(ProcessorContext context) {
        this.context = context;
        this.dateStore = context.getStateStore("dateStore");
    }

    @Override
    public String transform(String value) {
        final String output;
        if (dateStore.get(value) != null) {
            output = null; //discard the record
        } else {
            output = value;
            dateStore.put(value, value);
        }
        return output;
    }

    @Override
    public void close() {

    }
}
