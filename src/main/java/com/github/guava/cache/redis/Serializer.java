package com.github.guava.cache.redis;

/**
 * @author fanliwen
 */
public interface Serializer {

    byte[] serialize(final Object obj);

    <T> T deserialize(final byte[] objectData);
}
