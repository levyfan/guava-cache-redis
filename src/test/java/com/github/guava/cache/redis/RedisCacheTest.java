package com.github.guava.cache.redis;

import com.google.common.cache.CacheLoader;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.ExecutionException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

@Testcontainers
public class RedisCacheTest {

    @Container
    public GenericContainer redis = new GenericContainer(DockerImageName.parse("redis:7.0.2-bullseye"))
            .withExposedPorts(6379);

    private RedisCache<String, String> redisCache;

    @BeforeEach
    public void setup() {

        String address = redis.getHost();
        Integer port = redis.getFirstMappedPort();

        GenericObjectPoolConfig<Jedis> jedisConfig = new GenericObjectPoolConfig<>();

        JedisPool jedisPool = new JedisPool(jedisConfig, address, port);

        redisCache = new RedisCache<>(
                jedisPool,
                new JdkSerializer(),
                new JdkSerializer(),
                "test".getBytes(StandardCharsets.UTF_8),
                100,
                new CacheLoader<>() {
                    @Override
                    public String load(String key) throws Exception {
                        try (Jedis jedis = jedisPool.getResource()) {
                            return jedis.get(key);
                        }
                    }
                }
        );
    }

    @Test
    public void basicOperations() {
        String key = "key-" + System.currentTimeMillis();
        String value = "value-" + System.currentTimeMillis();

        assertNull(redisCache.getIfPresent(key));
        redisCache.put(key, value);

        assertEquals(value, redisCache.getIfPresent(key));

        redisCache.invalidate(key);
        assertNull(redisCache.getIfPresent(key));

    }

}
