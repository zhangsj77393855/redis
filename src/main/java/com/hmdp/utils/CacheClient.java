package com.hmdp.utils;

import cn.hutool.core.util.BooleanUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import static com.hmdp.utils.RedisConstants.CACHE_NULL_TTL;
import static com.hmdp.utils.RedisConstants.CACHE_SHOP_KEY;
import static com.hmdp.utils.RedisConstants.LOCK_SHOP_KEY;

@Slf4j
@Component
public class CacheClient {

    private final StringRedisTemplate stringRedisTemplate;

    private static final ExecutorService CACHE_REBUILD_EXECUTOR = Executors.newFixedThreadPool(10);

    @Autowired
    public CacheClient(StringRedisTemplate stringRedisTemplate) {
        this.stringRedisTemplate = stringRedisTemplate;
    }

    public void set(String key, Object value, Long time, TimeUnit unit) {
        stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(value), time, unit);
    }

    public void setWithLogicalExpire(String key, Object value, Long time, TimeUnit unit) {
        // 设置逻辑过期
        RedisData redisData = new RedisData();
        redisData.setData(value);
        redisData.setExpireTime(LocalDateTime.now().plusSeconds(unit.toSeconds(time)));
        // 写入Redis
        stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(redisData));
    }

    public <R,ID> R queryWithPassThrough(
            String keyPrefix, ID id, Class<R> type, Function<ID, R> dbFallback, Long time, TimeUnit unit){
        String key = keyPrefix + id;
        // 1.从redis查询商铺缓存
        String json = stringRedisTemplate.opsForValue().get(key);
        // 2.判断是否存在
        if (StrUtil.isNotBlank(json)) {
            // 3.存在，直接返回
            return JSONUtil.toBean(json, type);
        }
        // 判断命中的是否是空值
        if (json != null) {
            // 返回一个错误信息
            return null;
        }

        // 4.不存在，根据id查询数据库
        R r = dbFallback.apply(id);
        // 5.不存在，返回错误
        if (r == null) {
            // 将空值写入redis
            stringRedisTemplate.opsForValue().set(key, "", CACHE_NULL_TTL, TimeUnit.MINUTES);
            // 返回错误信息
            return null;
        }
        // 6.存在，写入redis
        this.set(key, r, time, unit);
        return r;
    }

    public <R, ID> R queryWithLogicalExpire(
            String keyPrefix, ID id, Class<R> type, Function<ID, R> dbFallback, Long time, TimeUnit unit) {
        String key = keyPrefix + id;
        // 1.从redis查询商铺缓存
        String json = stringRedisTemplate.opsForValue().get(key);
        // 2.判断是否存在
        if (StrUtil.isBlank(json)) {
            // 3.存在，直接返回
            return null;
        }
        // 4.命中，需要先把json反序列化为对象
        RedisData redisData = JSONUtil.toBean(json, RedisData.class);
        R r = JSONUtil.toBean((JSONObject) redisData.getData(), type);
        LocalDateTime expireTime = redisData.getExpireTime();
        // 5.判断是否过期
        if(expireTime.isAfter(LocalDateTime.now())) {
            // 5.1.未过期，直接返回店铺信息
            return r;
        }
        // 5.2.已过期，需要缓存重建
        // 6.缓存重建
        // 6.1.获取互斥锁
        String lockKey = LOCK_SHOP_KEY + id;
        boolean isLock = tryLock(lockKey);
        // 6.2.判断是否获取锁成功
        if (isLock){
            // 6.3.成功，开启独立线程，实现缓存重建
            CACHE_REBUILD_EXECUTOR.submit(() -> {
                try {
                    // 查询数据库
                    R newR = dbFallback.apply(id);
                    // 重建缓存
                    this.setWithLogicalExpire(key, newR, time, unit);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }finally {
                    // 释放锁
                    unlock(lockKey);
                }
            });
        }
        // 6.4.返回过期的商铺信息
        return r;
    }


    private <R,ID> R queryWithLogicExpire2(final String keyPrefix, ID id, Class<R> type, Function<ID,R> function, long time,TimeUnit unit)  {
        // 从redis中查询是否存在当前商品详情的id
        // 设置key
        final String shopKey = keyPrefix + id;
        final String redisValue = stringRedisTemplate.opsForValue().get(shopKey);
        // 此处有可能存在redisValue为空 那是为了应付穿透的情况 因此缓存空值
        if (StrUtil.isEmpty(redisValue)) {
            return null;
        }
        // 命中的情况
        final RedisData redisData = JSONUtil.toBean(redisValue, RedisData.class);
        final JSONObject data = (JSONObject) redisData.getData();
        final R bean = JSONUtil.toBean(data, type);
        if (redisData.getExpireTime().isAfter(LocalDateTime.now())) {
            // 如果没有过期 那么就直接返回
            return bean;
        }
        // 设置店铺锁的id
        final String lockId = "lock" + id;
        // 没有命中 重新开启线程
        if (tryLock2(lockId)) {
            // 拿到锁之后再次校验redis的缓存是否存在  如果没有再进行线程重启
            final String secondRedisValue = stringRedisTemplate.opsForValue().get(shopKey);
            final RedisData secondRedisData = JSONUtil.toBean(secondRedisValue, RedisData.class);
            if (secondRedisData == null) {
                return null;
            }
            if (secondRedisData.getExpireTime().isAfter(LocalDateTime.now())) {
                // 如果没有过期 那么就直接返回
                final JSONObject secondData = (JSONObject) secondRedisData.getData();
                return JSONUtil.toBean(secondData, type);
            }
            // 依旧是过期的 进行缓存重建
            CACHE_REBUILD_EXECUTOR.submit(() ->{
                final R redisDatas = function.apply(id);
                try {
                    this.saveRedisData(id,10L,redisDatas);
                } catch (Exception e) {
                    throw new RuntimeException();
                } finally {
                    // 释放锁
                    deleteLock(lockId);
                }
            });

        }
        return bean;
    }

    public <R, ID> R queryWithMutex(
            String keyPrefix, ID id, Class<R> type, Function<ID, R> dbFallback, Long time, TimeUnit unit) {
        String key = keyPrefix + id;
        // 1.从redis查询商铺缓存
        String shopJson = stringRedisTemplate.opsForValue().get(key);
        // 2.判断是否存在
        if (StrUtil.isNotBlank(shopJson)) {
            // 3.存在，直接返回
            return JSONUtil.toBean(shopJson, type);
        }
        // 判断命中的是否是空值
        if (shopJson != null) {
            // 返回一个错误信息
            return null;
        }

        // 4.实现缓存重建
        // 4.1.获取互斥锁
        String lockKey = LOCK_SHOP_KEY + id;
        R r = null;
        try {
            boolean isLock = tryLock(lockKey);
            // 4.2.判断是否获取成功
            if (!isLock) {
                // 4.3.获取锁失败，休眠并重试
                Thread.sleep(50);
                return queryWithMutex(keyPrefix, id, type, dbFallback, time, unit);
            }
            // 4.4.获取锁成功，根据id查询数据库
            r = dbFallback.apply(id);
            // 5.不存在，返回错误
            if (r == null) {
                // 将空值写入redis
                stringRedisTemplate.opsForValue().set(key, "", CACHE_NULL_TTL, TimeUnit.MINUTES);
                // 返回错误信息
                return null;
            }
            // 6.存在，写入redis
            this.set(key, r, time, unit);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }finally {
            // 7.释放锁
            unlock(lockKey);
        }
        // 8.返回
        return r;
    }

    private boolean tryLock(String key) {
        Boolean flag = stringRedisTemplate.opsForValue().setIfAbsent(key, "1", 10, TimeUnit.SECONDS);
        return BooleanUtil.isTrue(flag);
    }

    // 解决缓存穿透和击穿
    private <R,ID> R queryWithMutex2(final String keyPrefix,ID id,Class<R> type,Long time,TimeUnit unit,Function<ID,R> function) throws JsonProcessingException {
        final ObjectMapper objectMapper = new ObjectMapper();
        // 从redis中查询是否存在当前商品详情的id
        // 设置key
        final String shopKey = keyPrefix + id;
        final String redisValue = stringRedisTemplate.opsForValue().get(shopKey);
        // 此处有可能存在redisValue为空 那是为了应付穿透的情况 因此缓存空值
        if (StrUtil.isNotEmpty(redisValue)) {
            return JSONUtil.toBean(redisValue, type);
        }
        // 判断当前是redis中的穿透情景 因为缓存的是""
        if (redisValue != null) {
            return null;
        }

        // 设置店铺锁的id
        final String lockId = "lock" + id;
        final R shop;
        try {
            // 当前未命中缓存,尝试获取锁,然后判断是否需要休眠并且重新获取锁
            final boolean tryLock = tryLock2(lockId);
            // 如果没有获取到锁
            if (!tryLock) {
                Thread.sleep(50);
                this.queryWithMutex(keyPrefix,id,type,function,time,unit);
            }
            // 获取锁之后判断redis中是否有缓存,如果没有缓存则重建缓存
            final String secondRedisValue = stringRedisTemplate.opsForValue().get(shopKey);
            if (StrUtil.isNotEmpty(secondRedisValue)) {
                return objectMapper.readValue(secondRedisValue,type);
            }
            // 如果当前缓存不为空 说明还有可能进入穿透
            if (secondRedisValue != null) {
                return null;
            }
            // 未命中,重建缓存
            Thread.sleep(50);
            shop = function.apply(id);
            if (shop == null) {
                //4：没有
                stringRedisTemplate.opsForValue().set(shopKey, "", CACHE_NULL_TTL, TimeUnit.SECONDS);
                return null;
            }

            //5：有、将店铺数据写入redis中
            final String string = objectMapper.writeValueAsString(shop);
            stringRedisTemplate.opsForValue().set(shopKey , string, time, unit);
        } catch (InterruptedException e) {
            throw new RuntimeException();
        } finally {
            deleteLock(lockId);
        }
        return shop;
    }


    private boolean tryLock2(final String key) {
        final Boolean lockShop = stringRedisTemplate.opsForValue().setIfAbsent(key, "1");
        return BooleanUtil.isTrue(lockShop);
    }

    private void deleteLock(final String key){
        stringRedisTemplate.delete(key);
    }

    private <ID,R>void saveRedisData(final ID id, final long seconds, final R shop) throws InterruptedException {
        Thread.sleep(200);
        final RedisData redisData = RedisData.builder()
            .data(shop)
            .expireTime(LocalDateTime.now().plusSeconds(seconds))
            .build();
        final String jsonStr = JSONUtil.toJsonStr(redisData);
        stringRedisTemplate.opsForValue().set(CACHE_SHOP_KEY + id, jsonStr);
    }


    private void unlock(String key) {
        stringRedisTemplate.delete(key);
    }
}
