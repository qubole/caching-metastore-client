package com.qubole.utility;

import com.google.common.cache.CacheLoader;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import com.google.common.primitives.Bytes;
import org.apache.commons.lang3.SerializationUtils;

import org.junit.Assert;
import org.junit.Test;
import org.mockito.Matchers;
import org.mockito.Mockito;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.exceptions.JedisConnectionException;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;
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
    when(jedisPool.getResource()).thenThrow(new JedisConnectionException("Could not get a resource from the pool."));

    String keyPrefixStr = "test.";
    String key = "key";
    Integer valueExpected = 12;

    byte[] keyPrefix = keyPrefixStr.getBytes();
    int expiration = 1;
    int missingCacheExpiration = 1;
    boolean enableMissingCache = true;

    CacheLoader<String, Integer> cacheLoader = mock(CacheLoader.class);
    when(cacheLoader.load(key)).thenReturn(valueExpected);

    RedisCache redisCache = new RedisCache<>(jedisPool, keyPrefix, expiration, missingCacheExpiration, cacheLoader, enableMissingCache);
    Integer valueObserved = (Integer) redisCache.get(key);
    assert (valueExpected.equals(valueObserved));

  }

  //jedis.getResource() throws an error, getIfPresent should return empty and suppress exceptions
  @Test
  public void testGetIfPresentIfRedisDown()  {
    JedisPool jedisPool = mock(JedisPool.class);
    when(jedisPool.getResource()).thenThrow(new JedisConnectionException("Could not get a resource from the pool."));

    String keyPrefixStr = "test.";
    byte[] keyPrefix = keyPrefixStr.getBytes();
    int expiration = 1;
    int missingCacheExpiration = 1;
    boolean enableMissingCache = false;

    CacheLoader<String, Integer> cacheLoader = mock(CacheLoader.class);

    RedisCache redisCache = new RedisCache<>(jedisPool, keyPrefix, expiration, missingCacheExpiration, cacheLoader, enableMissingCache);
    Object valueObserved = redisCache.getIfPresent("key");
    assert (null == valueObserved);
  }

  //jedisPool.getResource() throws an error, value gets loaded from value Loader
  @Test
  public void testGetAllFromSourceIfRedisDown() throws Exception {
    JedisPool jedisPool = mock(JedisPool.class);
    when(jedisPool.getResource()).thenThrow(new JedisConnectionException("Could not get a resource from the pool."));

    String keyPrefixStr = "test.";
    Set<String> key = Sets.newLinkedHashSet(ImmutableList.of("key"));
    Map<String, Integer> valueExpected = ImmutableMap.of("key", 12);

    byte[] keyPrefix = keyPrefixStr.getBytes();
    int expiration = 1;
    int missingCacheExpiration = 1;
    boolean enableMissingCache = false;

    CacheLoader<String, Integer> cacheLoader = mock(CacheLoader.class);
    when(cacheLoader.loadAll(key)).thenReturn(valueExpected);

    RedisCache redisCache = new RedisCache<>(jedisPool, keyPrefix, expiration, missingCacheExpiration, cacheLoader, enableMissingCache);
    Map<String, Integer> valueObserved = redisCache.getAll(key);
    assert (valueExpected.equals(valueObserved));
  }

  //jedisPool.getResource() throws an error, invalidate should suppress exceptions
  @Test
  public void testInvalidateIfRedisDown() {
    JedisPool jedisPool = mock(JedisPool.class);
    when(jedisPool.getResource()).thenThrow(new JedisConnectionException("Could not get a resource from the pool."));

    String keyPrefixStr = "test.";
    byte[] keyPrefix = keyPrefixStr.getBytes();
    int expiration = 1;
    int missingCacheExpiration = 1;
    boolean enableMissingCache = false;

    RedisCache redisCache = new RedisCache<>(jedisPool, keyPrefix, expiration, missingCacheExpiration, mock(CacheLoader.class), enableMissingCache);
    redisCache.invalidate("key");
  }

  //jedisPool.getResource() throws an error, put should suppress exceptions
  @Test
  public void testPutIfRedisDown() {
    JedisPool jedisPool = mock(JedisPool.class);
    when(jedisPool.getResource()).thenThrow(new JedisConnectionException("Could not get a resource from the pool."));

    String keyPrefixStr = "test.";
    byte[] keyPrefix = keyPrefixStr.getBytes();
    int expiration = 1;
    int missingCacheExpiration = 1;
    boolean enableMissingCache = false;

    RedisCache redisCache = new RedisCache<>(jedisPool, keyPrefix, expiration, missingCacheExpiration, mock(CacheLoader.class), enableMissingCache);
    redisCache.put("key", 1);
  }

  //jedisPool.getResource() throws an error, invalidate should suppress exceptions
  @Test
  public void testPutAllIfRedisDown() {
    JedisPool jedisPool = mock(JedisPool.class);
    when(jedisPool.getResource()).thenThrow(new JedisConnectionException("Could not get a resource from the pool."));

    String keyPrefixStr = "test.";
    byte[] keyPrefix = keyPrefixStr.getBytes();
    int expiration = 1;
    int missingCacheExpiration = 1;
    boolean enableMissingCache = false;

    RedisCache redisCache = new RedisCache<>(jedisPool, keyPrefix, expiration, missingCacheExpiration, mock(CacheLoader.class), enableMissingCache);
    redisCache.putAll(ImmutableMap.of("key", 1));
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

  // Put should not be called if redis is down and underlying source returns a null
  @Test
  public void testGetException() throws Exception {
    JedisPool jedisPool = mock(JedisPool.class);
    when(jedisPool.getResource()).thenThrow(new JedisConnectionException("Could not get a resource from the pool."));

    String keyPrefixStr = "test.";
    String key = "key";
    byte[] keyPrefix = keyPrefixStr.getBytes();
    int expiration = 1;
    int missingCacheExpiration = 1;
    boolean enableMissingCache = true;

    CacheLoader<String, Integer> cacheLoader = mock(CacheLoader.class);
    RedisCache redisCache = new RedisCache<>(jedisPool, keyPrefix, expiration, missingCacheExpiration, cacheLoader, enableMissingCache);
    RedisCache spy = spy(redisCache);

    try {
      redisCache.get(key);
      Assert.fail("Expected InvalidCacheLoadException");
    } catch (CacheLoader.InvalidCacheLoadException e) {
      // Expected
    }
    verify(spy, Mockito.times(0)).put(Matchers.anyObject(), Matchers.anyObject());
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