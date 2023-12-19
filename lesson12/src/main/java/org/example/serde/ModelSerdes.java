package org.example.serde;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;

public class ModelSerdes {
    public static <T> Serde<T> serde(Class<T> cls) {
        return new Serdes.WrapperSerde<>(new JsonSerializer<>(), new JsonDeserializer<>(cls));
    }
}
