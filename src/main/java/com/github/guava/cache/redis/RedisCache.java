package com.github.guava.cache.redis;

import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.logging.Level;
import java.util.logging.Logger;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.Pipeline;

import com.google.common.cache.AbstractLoadingCache;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.google.common.primitives.Bytes;
import com.google.common.util.concurrent.ExecutionError;
import com.google.common.util.concurrent.UncheckedExecutionException;

/**
 * @author fanliwen
 */
public class RedisCache<K, V> extends AbstractLoadingCache<K, V> implements LoadingCache<K, V> {

    static final Logger logger = Logger.getLogger(RedisCache.class.getName());

    private final JedisPool jedisPool;

    private final Serializer keySerializer;

    private final Serializer valueSerializer;

    private final byte[] keyPrefix;

    private final int expiration;

    private final CacheLoader<K, V> loader;

    public RedisCache(
            JedisPool jedisPool,
            Serializer keySerializer,
            Serializer valueSerializer,
            byte[] keyPrefix,
            int expiration) {
        this(jedisPool, keySerializer, valueSerializer, keyPrefix, expiration, null);
    }

    public RedisCache(
            JedisPool jedisPool,
            Serializer keySerializer,
            Serializer valueSerializer,
            byte[] keyPrefix,
            int expiration,
            CacheLoader<K, V> loader) {
        this.jedisPool = jedisPool;
        this.keySerializer = keySerializer;
        this.valueSerializer = valueSerializer;
        this.keyPrefix = keyPrefix;
        this.expiration = expiration;
        this.loader = loader;
    }

    public V getIfPresent(Object o) {
        try (Jedis jedis = jedisPool.getResource()) {
            byte[] reply = jedis.get(Bytes.concat(keyPrefix, keySerializer.serialize(o)));
            if (reply == null) {
                return null;
            } else {
                return valueSerializer.deserialize(reply);
            }
        }
    }

    @Override
    public V get(K key, Callable<? extends V> valueLoader) throws ExecutionException {
        try {
            V value = this.getIfPresent(key);
            if (value == null) {
                value = valueLoader.call();
                if (value == null) {
                    throw new CacheLoader.InvalidCacheLoadException("valueLoader must not return null, key=" + key);
                } else {
                    this.put(key, value);
                }
            }
            return value;
        } catch (Throwable e) {
            convertAndThrow(e);
            // never execute
            return null;
        }
    }

    @Override
    public ImmutableMap<K, V> getAllPresent(Iterable<?> keys) {
        List<byte[]> keyBytes = new ArrayList<>();
        for (Object key : keys) {
            keyBytes.add(Bytes.concat(keyPrefix, keySerializer.serialize(key)));
        }

        try (Jedis jedis = jedisPool.getResource()) {
            List<byte[]> valueBytes = jedis.mget(Iterables.toArray(keyBytes, byte[].class));

            Map<K, V> map = new LinkedHashMap<>();
            int i = 0;
            for (Object key : keys) {
                @SuppressWarnings("unchecked")
                K castKey = (K) key;
                map.put(castKey, valueSerializer.<V>deserialize(valueBytes.get(i)));
                i++;
            }
            return ImmutableMap.copyOf(map);
        }
    }

    @Override
    public void put(K key, V value) {
        byte[] keyBytes = Bytes.concat(keyPrefix, keySerializer.serialize(key));
        byte[] valueBytes = valueSerializer.serialize(value);

        try (Jedis jedis = jedisPool.getResource()) {
            if (expiration > 0) {
                jedis.setex(keyBytes, expiration, valueBytes);
            } else {
                jedis.set(keyBytes, valueBytes);
            }
        }
    }

    @Override
    public void putAll(Map<? extends K, ? extends V> m) {
        List<byte[]> keysvalues = new ArrayList<>();
        for (Map.Entry<? extends K, ? extends V> entry : m.entrySet()) {
            keysvalues.add(Bytes.concat(keyPrefix, keySerializer.serialize(entry.getKey())));
            keysvalues.add(valueSerializer.serialize(entry.getValue()));
        }

        try (Jedis jedis = jedisPool.getResource()) {
            if (expiration > 0) {
                Pipeline pipeline = jedis.pipelined();
                jedis.mset(Iterables.toArray(keysvalues, byte[].class));
                for (int i = 0; i < keysvalues.size(); i += 2) {
                    jedis.expire(keysvalues.get(i), expiration);
                }
                pipeline.exec();
            } else {
                jedis.mset(Iterables.toArray(keysvalues, byte[].class));
            }
        }
    }

    @Override
    public void invalidate(Object key) {
        try (Jedis jedis = jedisPool.getResource()) {
            jedis.del(Bytes.concat(keyPrefix, keySerializer.serialize(key)));
        }
    }

    @Override
    public void invalidateAll(Iterable<?> keys) {
        Set<byte[]> keyBytes = new LinkedHashSet<>();
        for (Object key : keys) {
            keyBytes.add(Bytes.concat(keyPrefix, keySerializer.serialize(key)));
        }

        try (Jedis jedis = jedisPool.getResource()) {
            jedis.del(Iterables.toArray(keyBytes, byte[].class));
        }
    }

    @Override
    public V get(final K key) throws ExecutionException {
        return this.get(key, new Callable<V>() {
            @Override
            public V call() throws Exception {
                return loader.load(key);
            }
        });
    }

    @Override
    public ImmutableMap<K, V> getAll(Iterable<? extends K> keys) throws ExecutionException {
        Map<K, V> result = Maps.newLinkedHashMap(this.getAllPresent(keys));

        Set<K> keysToLoad = Sets.newLinkedHashSet(keys);
        keysToLoad.removeAll(result.keySet());
        if (!keysToLoad.isEmpty()) {
            try {
                Map<K, V> newEntries = loader.loadAll(keysToLoad);
                if (newEntries == null) {
                    throw new CacheLoader.InvalidCacheLoadException(loader + " returned null map from loadAll");
                }
                this.putAll(newEntries);

                for (K key : keysToLoad) {
                    V value = newEntries.get(key);
                    if (value == null) {
                        throw new CacheLoader.InvalidCacheLoadException("loadAll failed to return a value for " + key);
                    }
                    result.put(key, value);
                }
            } catch (CacheLoader.UnsupportedLoadingOperationException e) {
                Map<K, V> newEntries = new LinkedHashMap<>();
                boolean nullsPresent = false;
                Throwable t = null;
                for (K key : keysToLoad) {
                    try {
                        V value = loader.load(key);
                        if (value == null) {
                            // delay failure until non-null entries are stored
                            nullsPresent = true;
                        } else {
                            newEntries.put(key, value);
                        }
                    } catch (Throwable tt) {
                        t = tt;
                        break;
                    }
                }
                this.putAll(newEntries);

                if (nullsPresent) {
                    throw new CacheLoader.InvalidCacheLoadException(loader + " returned null keys or values from loadAll");
                } else if (t != null) {
                    convertAndThrow(t);
                } else {
                    result.putAll(newEntries);
                }
            } catch (Throwable e) {
                convertAndThrow(e);
            }
        }

        return ImmutableMap.copyOf(result);
    }

    @Override
    public void refresh(K key) {
        try {
            V value = loader.load(key);
            this.put(key, value);
        } catch (Exception e) {
            logger.log(Level.WARNING, "Exception thrown during refresh", e);
        }
    }

    private static void convertAndThrow(Throwable t) throws ExecutionException {
        if (t instanceof InterruptedException) {
            Thread.currentThread().interrupt();
            throw new ExecutionException(t);
        } else if (t instanceof RuntimeException) {
            throw new UncheckedExecutionException(t);
        } else if (t instanceof Exception) {
            throw new ExecutionException(t);
        } else {
            throw new ExecutionError((Error) t);
        }
    }
}
