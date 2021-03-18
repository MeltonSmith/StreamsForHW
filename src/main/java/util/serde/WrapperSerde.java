package util.serde;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;
import util.JsonDeserializer;
import util.JsonSerializer;

/**
 * Created by: Ian_Rakhmatullin
 * Date: 18.03.2021
 */
public abstract class WrapperSerde<T> implements Serde<T> {
    private JsonSerializer<T> serializer;
    private JsonDeserializer<T> deserializer;

    protected WrapperSerde(JsonSerializer<T> serializer, JsonDeserializer<T> deserializer) {
        this.serializer = serializer;
        this.deserializer = deserializer;
    }

    @Override
    public Serializer<T> serializer() {
        return serializer;
    }

    @Override
    public Deserializer<T> deserializer() {
        return deserializer;
    }
}
