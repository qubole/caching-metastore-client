package com.qubole.utility;

import com.google.common.cache.CacheLoader;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.primitives.Bytes;
import org.apache.commons.lang3.SerializationUtils;

import org.junit.Test;
import org.mockito.Matchers;
import org.mockito.Mockito;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.exceptions.JedisConnectionException;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;

import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.verify;


public class RedisCacheTest {


  //Value gets loaded from the Cache
  @Test
  public void testGetFromCache() throws Exception {

    String keyPrefixStr = "test.";
    String key = "key";
    Integer sourceClientValue = 12;
    Integer cacheValue = 50;

    JedisPool jedisPool = mock(JedisPool.class);
    Jedis jedis = mock(Jedis.class);
    when(jedisPool.getResource()).thenReturn(jedis);
    when(jedis.get((keyPrefixStr + key).getBytes())).thenReturn(SerializationUtils.serialize((Serializable) cacheValue));

    byte[] keyPrefix = keyPrefixStr.getBytes();
    int expiration = 1;
    int missingCacheExpiration = 1;
    boolean enableMissingCache = false;

    CacheLoader<String, Integer> cacheLoader = mock(CacheLoader.class);
    when(cacheLoader.load(key)).thenReturn(sourceClientValue);

    RedisCache redisCache = new RedisCache<>(jedisPool, keyPrefix, expiration, missingCacheExpiration, cacheLoader, enableMissingCache);
    Integer valueObserved = (Integer) redisCache.get(key);
    assert (cacheValue.equals(valueObserved));

  }

  //jedis.get() throws an error, value gets loaded from value Loader
  @Test
  public void testGetFromSource() throws Exception {

    JedisPool jedisPool = mock(JedisPool.class);
    Jedis jedis = mock(Jedis.class);
    when(jedis.get(anyString().getBytes())).thenThrow(new JedisConnectionException("Get call failed."));
    when(jedisPool.getResource()).thenReturn(jedis);

    String keyPrefixStr = "test.";
    String key = "key";
    Integer valueExpected = 12;

    byte[] keyPrefix = keyPrefixStr.getBytes();
    int expiration = 1;
    int missingCacheExpiration = 1;
    boolean enableMissingCache = false;

    CacheLoader<String, Integer> cacheLoader = mock(CacheLoader.class);
    when(cacheLoader.load(key)).thenReturn(valueExpected);

    RedisCache redisCache = new RedisCache<>(jedisPool, keyPrefix, expiration, missingCacheExpiration, cacheLoader, enableMissingCache);
    Integer valueObserved = (Integer) redisCache.get(key);
    assert (valueExpected.equals(valueObserved));

  }

  //jedis.get() returns null, value gets loaded from value Loader
  @Test
  public void testGetFromSourceCacheMiss() throws Exception {

    JedisPool jedisPool = mock(JedisPool.class);
    Jedis jedis = mock(Jedis.class);
    when(jedis.get(anyString().getBytes())).thenReturn(null);
    when(jedisPool.getResource()).thenReturn(jedis);

    String keyPrefixStr = "test.";
    String key = "key";
    Integer valueExpected = 12;

    byte[] keyPrefix = keyPrefixStr.getBytes();
    int expiration = 1;
    int missingCacheExpiration = 1;
    boolean enableMissingCache = false;

    CacheLoader<String, Integer> cacheLoader = mock(CacheLoader.class);
    when(cacheLoader.load(key)).thenReturn(valueExpected);

    RedisCache redisCache = new RedisCache<>(jedisPool, keyPrefix, expiration, missingCacheExpiration, cacheLoader, enableMissingCache);
    Integer valueObserved = (Integer) redisCache.get(key);
    assert (valueExpected.equals(valueObserved));

  }


  //Exception thrown if missingcache is enabled, and the key exists in "missing" cache.
  @Test(expected = ExecutionException.class)
  public void testGetFromMissingCache() throws Exception {
    String keyPrefixStr = "test.";
    String key = "key";

    JedisPool jedisPool = mock(JedisPool.class);
    Jedis jedis = mock(Jedis.class);
    when(jedisPool.getResource()).thenReturn(jedis);

    byte[] keyPrefix = keyPrefixStr.getBytes();
    int expiration = 1;
    int missingCacheExpiration = 1;
    boolean enableMissingCache = true;

    CacheLoader<String, Integer> cacheLoader = mock(CacheLoader.class);

    RedisCache redisCache = new RedisCache<>(jedisPool, keyPrefix, expiration, missingCacheExpiration, cacheLoader, enableMissingCache);
    when(jedis.exists(Bytes.concat(redisCache.getNotFoundPrefix(), key.getBytes()))).thenReturn(true);

    redisCache.get(key);
  }

  //Put not called if get method throws exception
  @Test
  public void testGetException() throws Exception {
    String key = "key";
    Integer valueExpected = 12;

    Callable<Integer> valueLoader = mock(Callable.class);
    when(valueLoader.call()).thenReturn(valueExpected);

    RedisCache redisCache = mock(RedisCache.class);
    when(redisCache.getIfPresent(Matchers.anyObject())).thenThrow(new JedisConnectionException("Get call failed."));
    doCallRealMethod().when(redisCache).get(key, valueLoader);
    doCallRealMethod().when(redisCache).getFromSource(key, valueLoader, true);
    redisCache.get(key, valueLoader);
    verify(redisCache, Mockito.times(0)).put(Matchers.anyObject(), Matchers.anyObject());
  }

  @Test
  public void testGetAllPresentWithNoMatches() throws Exception {
    String keyPrefixStr = "test.";
    String key = "key";

    JedisPool jedisPool = mock(JedisPool.class);
    Jedis jedis = mock(Jedis.class);
    when(jedisPool.getResource()).thenReturn(jedis);

    byte[] keyPrefix = keyPrefixStr.getBytes();
    int expiration = 1;
    int missingCacheExpiration = 1;
    boolean enableMissingCache = false;

    CacheLoader<String, Integer> cacheLoader = mock(CacheLoader.class);
    RedisCache redisCache = new RedisCache<>(jedisPool, keyPrefix, expiration, missingCacheExpiration, cacheLoader, enableMissingCache);

    List<byte[]> keyBytes = new ArrayList<>();
    JdkSerializer<String> serializer = new JdkSerializer<>();
    keyBytes.add(Bytes.concat(keyPrefix, serializer.serialize(key)));
    List<byte[]> expectedValues = new ArrayList<>();
    expectedValues.add(null);
    when(jedis.mget(Iterables.toArray(keyBytes, byte[].class))).thenReturn(expectedValues);

    Set<String> keys = new HashSet<>();
    keys.add(key);
    Iterable<String> iterables = keys;
    ImmutableMap values = redisCache.getAllPresent(iterables);

    //expect a empty map.
    assert (values.size() == 0);
  }
}