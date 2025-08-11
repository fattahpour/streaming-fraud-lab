package com.fraudlab.serde;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.io.IOException;

public class JsonSerde<T> implements SerializationSchema<T>, DeserializationSchema<T> {
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private final Class<T> clazz;

    public JsonSerde(Class<T> clazz) {
        this.clazz = clazz;
    }

    @Override
    public byte[] serialize(T t) {
        try {
            return MAPPER.writeValueAsBytes(t);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public T deserialize(byte[] bytes) throws IOException {
        return MAPPER.readValue(bytes, clazz);
    }

    @Override
    public boolean isEndOfStream(T t) { return false; }

    @Override
    public TypeInformation<T> getProducedType() {
        return TypeInformation.of(clazz);
    }
}
