package com.flipkart.datastore;

/**
 * Created by dhritiman.das on 4/14/16.
 */


import java.util.*;
import javax.annotation.Nullable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.Response;
import redis.clients.jedis.exceptions.JedisConnectionException;

import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

import com.yammer.metrics.Metrics;
import com.yammer.metrics.core.Counter;
import com.yammer.metrics.core.Timer;


public class JedisHelper {
    private static final Logger LOG = LoggerFactory.getLogger(JedisHelper.class);
    private final Counter activeResources;
    private final Counter fetchedResources;
    private final JedisPool jedisPool;
    private final List<JedisPool> readOnlyPools;
    private final int numTries;
    private final static Function<Response<String>, String> valueToString = new Function<Response<String>, String>() {
        @Nullable
        @Override
        public String apply(@Nullable Response<String> input) {
            return input == null ? null : input.get();
        }
    };

    private final static Predicate<Map.Entry<String, String>> notNull = new Predicate<Map.Entry<String, String>>() {
        @Override
        public boolean apply(@Nullable Map.Entry<String, String> input) {
            return input != null;
        }
    };

    public class JedisPoolDelegator {
        private Jedis defaultResource;
        private Jedis readOnlyResource;
        private transient JedisPool jedisReadOnlyPool;

        public JedisPoolDelegator(JedisPool jedisReadOnlyPool) {
            this.jedisReadOnlyPool = jedisReadOnlyPool;
        }

        public Jedis getDefaultClient() {
            fetchedResources.inc();
            if(defaultResource == null) {
                defaultResource =  jedisPool.getResource();
                activeResources.inc();
            }
            return defaultResource;
        }

        public Jedis getReadOnlyClient() {
            fetchedResources.inc();
            if(readOnlyResource == null) {
                readOnlyResource =  this.jedisReadOnlyPool.getResource();
                activeResources.inc();
            }
            return readOnlyResource;
        }

        public void returnResource() {
            if(defaultResource != null)
                jedisPool.returnResource(defaultResource);
            if(readOnlyResource != null)
                this.jedisReadOnlyPool.returnResource(readOnlyResource);
        }

        public void returnBrokenResource() {
            if(defaultResource != null)
                jedisPool.returnBrokenResource(defaultResource);
            if(readOnlyResource != null)
                this.jedisReadOnlyPool.returnBrokenResource(readOnlyResource);
        }

    }

    public JedisHelper(JedisPool jedisPool, int numTries) {
        this(jedisPool, jedisPool, numTries);
    }

    public JedisHelper(JedisPool jedisPool, JedisPool readOnlyJedisPool, int numTries) {
        this(jedisPool, readOnlyJedisPool, readOnlyJedisPool, numTries);
    }

    public JedisHelper(JedisPool jedisPool, JedisPool readOnlyJedisPool, JedisPool readOnlyJedisPool2, int numTries) {
        //Assert.notNull(jedisPool);
        //Assert.notNull(readOnlyJedisPool);
        this.jedisPool = jedisPool;
        readOnlyPools = Lists.newArrayList();
        if(readOnlyJedisPool != null) {
            readOnlyPools.add(readOnlyJedisPool);
        }
        if(readOnlyJedisPool2 != null){
            readOnlyPools.add(readOnlyJedisPool2);
        }

        this.numTries = numTries;
        activeResources = Metrics.newCounter(this.getClass(),"activeResources");
        fetchedResources = Metrics.newCounter(this.getClass(),"fetchedResources");
    }

    public interface Callback<T> {
        T doWithJedis(JedisPoolDelegator poolDelegator);
    }


    public <T> T exec(Callback<T> jc) {

        JedisConnectionException lastException = null;
        Iterator<JedisPool> readOnlyPoolIterator= Iterators.cycle(readOnlyPools);

        for (int numTry = 0; numTry < numTries; ++numTry) {
            JedisPoolDelegator poolDelegator = this.new JedisPoolDelegator(readOnlyPoolIterator.next());
            boolean broken = false;

            try {
                return jc.doWithJedis(poolDelegator);
            } catch (JedisConnectionException e) {
                broken = true;
                lastException = e;
                LOG.warn("Connection error - try #{}, message - {}", numTry, e.getMessage());
            } finally {
                if (broken) {
                    poolDelegator.returnBrokenResource();
                    fetchedResources.dec();
                } else {
                    poolDelegator.returnResource();
                    activeResources.dec();
                    fetchedResources.dec();
                }
            }
        }

        throw lastException;
    }

    public Long hset(final String key, final String field, final String value) {
        return exec(new Callback<Long>() {
            public Long doWithJedis(JedisPoolDelegator poolDelegator) {
                return poolDelegator.getDefaultClient().hset(key, field, value);
            }
        });
    }

    public Long hdel(final String key, final String field) {
        return exec(new Callback<Long>() {
            public Long doWithJedis(JedisPoolDelegator poolDelegator) {
                return poolDelegator.getDefaultClient().hdel(key, field);
            }
        });
    }

    public class RedisMultiGet {
        private Map<String, String> keyFields;

        public RedisMultiGet() {
            this.keyFields = new HashMap<String, String>();
        }

        public RedisMultiGet add(String key, String field) {
            this.keyFields.put(key,field);
            return this;
        }

        public Map<String, String> fetch() {
            return exec(new Callback<Map<String,String>>() {
                public Map<String, String> doWithJedis(JedisPoolDelegator poolDelegator) {
                    Pipeline pipeline = poolDelegator.getReadOnlyClient().pipelined();
                    Map<String,Response<String>> response = Maps.newHashMap();
                    for(Map.Entry<String, String> entry : keyFields.entrySet()) {
                        response.put(entry.getKey(), pipeline.hget(entry.getKey(), entry.getValue()));
                    }
                    pipeline.sync();
                    return Maps.filterEntries(Maps.transformValues(response, valueToString), notNull);
                }
            });
        }

    }

    public final RedisMultiGet createPipeline(){
        return  new RedisMultiGet();
    }

    public final Map<String, String> pipelineHget(final Map<String, String> keyFields) {
        return exec(new Callback<Map<String,String>>() {
            public Map<String, String> doWithJedis(JedisPoolDelegator poolDelegator) {
                Pipeline pipeline = poolDelegator.getReadOnlyClient().pipelined();
                Map<String,Response<String>> response = Maps.newHashMap();
                for(Map.Entry<String, String> entry : keyFields.entrySet()) {
                    response.put(entry.getKey(), pipeline.hget(entry.getKey(), entry.getValue()));
                }
                pipeline.sync();
                return Maps.filterEntries(Maps.transformValues(response, valueToString), notNull);
            }
        });
    }

    public Set<String> keys( final String pattern ){
        return exec(new Callback<Set<String> >() {
            public Set<String> doWithJedis(JedisPoolDelegator poolDelegator) {
                return poolDelegator.getReadOnlyClient().keys( pattern );
            }
        });
    }


    public String hget(final String key, final String field) {
        return exec(new Callback<String>() {
            public String doWithJedis(JedisPoolDelegator poolDelegator) {
                return poolDelegator.getReadOnlyClient().hget(key, field);
            }
        });
    }


    public Map<String, String> hgetAll(final String key) {
        return exec(new Callback<Map<String, String>>() {
            public Map<String, String> doWithJedis(JedisPoolDelegator poolDelegator) {
                return poolDelegator.getReadOnlyClient().hgetAll(key);
            }
        });
    }


    public Boolean hexists(final String key, final String field) {
        return exec(new Callback<Boolean>() {
            public Boolean doWithJedis(JedisPoolDelegator poolDelegator) {
                return poolDelegator.getReadOnlyClient().hexists(key, field);
            }
        });
    }

    public Long hincrBy(final String key, final String field, final long value) {
        return exec(new Callback<Long>() {
            public Long doWithJedis(JedisPoolDelegator poolDelegator) {
                return poolDelegator.getDefaultClient().hincrBy(key, field, value);
            }
        });
    }

    public Long del(final String key) {
        return exec(new Callback<Long>() {
            public Long doWithJedis(JedisPoolDelegator poolDelegator) {
                return poolDelegator.getDefaultClient().del(key);
            }
        });
    }


    public void set(final String key, final String value) {
        exec(new Callback<String>() {
            public String doWithJedis(JedisPoolDelegator poolDelegator) {
                return poolDelegator.getDefaultClient().set(key, value);
            }
        });
    }

    public String get(final String key) {
        return exec(new Callback<String>() {
            public String doWithJedis(JedisPoolDelegator poolDelegator) {
                return poolDelegator.getReadOnlyClient().get(key);
            }
        });
    }

    public Boolean sismember(final String key, final String field) {
        return exec(new Callback<Boolean>() {
            public Boolean doWithJedis(JedisPoolDelegator poolDelegator) {
                return poolDelegator.getReadOnlyClient().sismember(key, field);
            }
        });
    }

    public Long sadd(final String key, final String field) {
        return exec(new Callback<Long>() {
            public Long doWithJedis(JedisPoolDelegator poolDelegator) {
                return poolDelegator.getDefaultClient().sadd(key, field);
            }
        });
    }
}
