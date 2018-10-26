/*
 *  Copyright (c) 2016 Liwen Fan

 *  Permission is hereby granted, free of charge, to any person obtaining a copy
 *  of this software and associated documentation files (the "Software"), to deal
 *  in the Software without restriction, including without limitation the rights
 *  to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 *  copies of the Software, and to permit persons to whom the Software is
 *  furnished to do so, subject to the following conditions:

 *  The above copyright notice and this permission notice shall be included in all
 *  copies or substantial portions of the Software.

 *  THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 *  IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 *  FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 *  AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 *  LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 *  OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 *  SOFTWARE.
 */

package com.qubole.utility;

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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.Pipeline;

/**
 * Created by sakshibansal on 28/04/17.
 */

/**
 * @param <K> Type: Key
 * @param <V> Type: Value
 */
public class RedisCache<K, V> extends AbstractLoadingCache<K, V> implements LoadingCache<K, V> {

  static final Logger LOGGER = LoggerFactory.getLogger(RedisCache.class);

  private static JedisPool jedisPool;

  private final Serializer keySerializer;

  private final Serializer valueSerializer;

  private final byte[] keyPrefix;

  private final byte[] notFoundPrefix;

  private final int missingCacheExpiration;

  private final int expiration;

  private final boolean enableMissingCache;

  private final CacheLoader<K, V> loader;

  public RedisCache(
          JedisPool jedisPool,
          byte[] keyPrefix,
          int expiration, int missingCacheExpiration,
          CacheLoader<K, V> loader, boolean enableMissingCache) {
    this.jedisPool = jedisPool;
    this.keySerializer = new JdkSerializer<K>();
    this.valueSerializer = new JdkSerializer<V>();
    this.keyPrefix = keyPrefix;
    this.notFoundPrefix = Bytes.concat("Doesn'tExist.".getBytes(), keyPrefix);
    this.expiration = expiration;
    this.loader = loader;
    this.enableMissingCache = enableMissingCache;
    this.missingCacheExpiration = missingCacheExpiration;
  }

  public V getIfPresent(Object o) {
    try (Jedis jedis = jedisPool.getResource()) {
      byte[] key = Bytes.concat(keyPrefix, keySerializer.serialize(o));
      byte[] reply = jedis.get(key);
      if (reply == null) {
        LOGGER.info("cache miss, key: " + new String(key));
        return null;
      } else {
        LOGGER.info("cache hit, key: " + new String(key));
        return valueSerializer.deserialize(reply);
      }
    } catch (Exception e) {
      LOGGER.warn("getIfPresent failed for key: " + o + " with exception: " + e.toString());
      return null;
    }
  }

  public boolean checkMissingCache(Object o) {
    try (Jedis jedis = jedisPool.getResource()) {
      byte[] key = Bytes.concat(notFoundPrefix, keySerializer.serialize(o));
      boolean reply = jedis.exists(key);
      if (!reply) {
        LOGGER.info("cache miss, key: " + new String(key));
      } else {
        LOGGER.info("cache hit, key: " + new String(key));
      }
      return reply;
    } catch (Exception e) {
      LOGGER.warn("checkMissingCache failed for key: " + o + " with exception: " + e.toString());
      return false;
    }
  }


  @Override
  public V get(K key, Callable<? extends V> valueLoader) throws ExecutionException {
    V value = getIfPresent(key);

    //get value from valueloader and update cache accordingly.
    if (value == null) {
      //Missing cache is enabled and the key is present in missing cache.
      if (enableMissingCache && checkMissingCache(key)) {
        convertAndThrow(
                new ExecutionException("key found in missing cache, key doesn't exist.", null));
      } else {
        //Either the missing cache is not enabled
        //or, it's enabled and the key is not present in missing cache
        value = getFromSource(key, valueLoader);
      }
    }
    return value;
  }

  public V getFromSource(K key, Callable<? extends V> valueLoader)
          throws ExecutionException {

    V value = null;
    try {
      value = valueLoader.call();
    } catch (Throwable e) {
      if (enableMissingCache) {
        this.putNotFound(key, "Does not exist.");
      }
      convertAndThrow(e);
    }
    if (value == null) {
      throw new CacheLoader
              .InvalidCacheLoadException("valueLoader must not return null, key=" + key);
    }

    this.put(key, value);
    return value;
  }


  @Override
  public ImmutableMap<K, V> getAllPresent(Iterable<?> keys) {
    try (Jedis jedis = jedisPool.getResource()) {
      List<byte[]> keyBytes = new ArrayList<>();
      for (Object key : keys) {
        keyBytes.add(Bytes.concat(keyPrefix, keySerializer.serialize(key)));
      }
      List<byte[]> valueBytes = jedis.mget(Iterables.toArray(keyBytes, byte[].class));

      Map<K, V> map = new LinkedHashMap<>();
      int i = 0;
      // mget always return an array of keys size.
      // Each entry would correspond the key of that index.
      // if no such key exists vaue will be null.

      for (Object key : keys) {
        if (valueBytes.get(i) != null) {
          @SuppressWarnings("unchecked")
          K castKey = (K) key;
          map.put(castKey, valueSerializer.<V>deserialize(valueBytes.get(i)));
          i++;
        }
      }
      return ImmutableMap.copyOf(map);
    } catch (Exception e) {
      LOGGER.error("Exception in getAllPresent: " + e.toString());
      return ImmutableMap.of();
    }
  }

  @Override
  public void put(K key, V value) {
    try (Jedis jedis = jedisPool.getResource()) {
      byte[] keyBytes = Bytes.concat(keyPrefix, keySerializer.serialize(key));
      byte[] valueBytes = valueSerializer.serialize(value);
      if (expiration > 0) {
        jedis.setex(keyBytes, expiration, valueBytes);
      } else {
        jedis.set(keyBytes, valueBytes);
      }
    } catch (Exception e) {
      LOGGER.error("Failed to put the key: " + key + ", with exception: " + e.toString());
    }
  }


  public void putNotFound(K key, String value) {
    try (Jedis jedis = jedisPool.getResource()) {
      byte[] keyBytes = Bytes.concat(notFoundPrefix, keySerializer.serialize(key));
      byte[] valueBytes = value.getBytes();
      if (missingCacheExpiration > 0) {
        jedis.setex(keyBytes, missingCacheExpiration, valueBytes);
      } else {
        jedis.set(keyBytes, valueBytes);
      }
    } catch (Exception e) {
      LOGGER.error("Error in putting the key to not-found redis");
    }
  }

  @Override
  public void putAll(Map<? extends K, ? extends V> m) {
    try (Jedis jedis = jedisPool.getResource()) {
      List<byte[]> keysvalues = new ArrayList<>();
      for (Map.Entry<? extends K, ? extends V> entry : m.entrySet()) {
        keysvalues.add(Bytes.concat(keyPrefix, keySerializer.serialize(entry.getKey())));
        keysvalues.add(valueSerializer.serialize(entry.getValue()));
      }

      if (expiration > 0) {
        Pipeline pipeline = jedis.pipelined();
        pipeline.mset(Iterables.toArray(keysvalues, byte[].class));
        for (int i = 0; i < keysvalues.size(); i += 2) {
          pipeline.expire(keysvalues.get(i), expiration);
        }
        pipeline.sync();
      } else {
        jedis.mset(Iterables.toArray(keysvalues, byte[].class));
      }
    } catch (Exception e) {
      LOGGER.error("Exception in putAll: " + e.toString());
    }
  }

  @Override
  public void invalidate(Object key) {
    try (Jedis jedis = jedisPool.getResource()) {
      jedis.del(Bytes.concat(keyPrefix, keySerializer.serialize(key)));
    } catch (Exception e) {
      LOGGER.error("Failed to invalidate key: " + key + " with exception: " + e.toString());
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
          throw new CacheLoader
                  .InvalidCacheLoadException(loader + " returned null map from loadAll");
        }
        this.putAll(newEntries);

        for (K key : keysToLoad) {
          V value = newEntries.get(key);
          if (value == null) {
            throw new CacheLoader
                    .InvalidCacheLoadException("loadAll failed to return a value for " + key);
          }
          result.put(key, value);
        }
      } catch (CacheLoader.InvalidCacheLoadException e) {
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
          throw new CacheLoader
                  .InvalidCacheLoadException(loader + " returned null keys or values from loadAll");
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
      LOGGER.warn("Exception thrown during refresh", e);
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

  byte[] getNotFoundPrefix() {
    return this.notFoundPrefix;
  }

}
