package com.github.guava.cache.redis;

import java.io.Serializable;

import org.apache.commons.lang3.SerializationUtils;

/**
 * @author fanliwen
 */
public class JdkSerializer implements Serializer {

    @Override
    public byte[] serialize(Object obj) {
        return SerializationUtils.serialize((Serializable) obj);
    }

    @Override
    public <T> T deserialize(byte[] objectData) {
        return SerializationUtils.deserialize(objectData);
    }
}
