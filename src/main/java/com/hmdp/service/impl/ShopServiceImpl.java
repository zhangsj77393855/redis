package com.hmdp.service.impl;

import cn.hutool.core.util.BooleanUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.hmdp.dto.Result;
import com.hmdp.entity.Shop;
import com.hmdp.mapper.ShopMapper;
import com.hmdp.service.IShopService;
import com.hmdp.utils.CacheClient;
import com.hmdp.utils.RedisData;
import com.hmdp.utils.SystemConstants;
import org.springframework.data.geo.Distance;
import org.springframework.data.geo.GeoResult;
import org.springframework.data.geo.GeoResults;
import org.springframework.data.redis.connection.RedisGeoCommands;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.domain.geo.GeoReference;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.Resource;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static com.hmdp.utils.RedisConstants.CACHE_SHOP_KEY;
import static com.hmdp.utils.RedisConstants.CACHE_SHOP_TTL;
import static com.hmdp.utils.RedisConstants.SHOP_GEO_KEY;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
public class ShopServiceImpl extends ServiceImpl<ShopMapper, Shop> implements IShopService {


    @Resource
    private StringRedisTemplate stringRedisTemplate;

    @Resource
    private CacheClient cacheClient;

    @Override
    public Result queryById(Long id) {
        // 解决缓存穿透
        Shop shop = cacheClient
                .queryWithPassThrough(CACHE_SHOP_KEY, id, Shop.class, this::getById, CACHE_SHOP_TTL, TimeUnit.MINUTES);

        // 互斥锁解决缓存击穿
        // Shop shop = cacheClient
        //         .queryWithMutex(CACHE_SHOP_KEY, id, Shop.class, this::getById, CACHE_SHOP_TTL, TimeUnit.MINUTES);

        // 逻辑过期解决缓存击穿
        // Shop shop = cacheClient
        //         .queryWithLogicalExpire(CACHE_SHOP_KEY, id, Shop.class, this::getById, 20L, TimeUnit.SECONDS);

        if (shop == null) {
            return Result.fail("店铺不存在！");
        }
        // 7.返回
        return Result.ok(shop);
    }

    private static final ExecutorService CACHE_EXECUTOR_REBUILD = Executors.newFixedThreadPool(10);

    private Shop queryWithLogicExpire(long id) {
        // 从redis中查询是否存在当前商品详情的id
        // 设置key
        final String shopKey = CACHE_SHOP_KEY + id;
        final String redisValue = stringRedisTemplate.opsForValue().get(shopKey);
        // 此处有可能存在redisValue为空 那是为了应付穿透的情况 因此缓存空值
        if (StrUtil.isEmpty(redisValue)) {
            return null;
        }
        // 命中的情况
        final RedisData redisData = JSONUtil.toBean(redisValue, RedisData.class);
        final JSONObject data = (JSONObject) redisData.getData();
        final Shop bean = JSONUtil.toBean(data, Shop.class);
        if (redisData.getExpireTime().isAfter(LocalDateTime.now())) {
            // 如果没有过期 那么就直接返回
            return bean;
        }
        // 设置店铺锁的id
        final String lockId = "lock" + id;
        // 没有命中 重新开启线程
        if (tryLock(lockId)) {
            // 拿到锁之后再次校验redis的缓存是否存在  如果没有再进行线程重启
            final String secondRedisValue = stringRedisTemplate.opsForValue().get(shopKey);
            final RedisData secondRedisData = JSONUtil.toBean(secondRedisValue, RedisData.class);
            if (secondRedisData.getExpireTime().isAfter(LocalDateTime.now())) {
                // 如果没有过期 那么就直接返回
                final JSONObject secondData = (JSONObject) secondRedisData.getData();
                return JSONUtil.toBean(secondData, Shop.class);
            }
            // 依旧是过期的 进行缓存重建
            CACHE_EXECUTOR_REBUILD.submit(() ->{
                final Shop byId = getById(id);
                try {
                    this.saveRedisData(id,10L,byId);
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

    private void saveRedisData(final long id, final long seconds, final Shop shop) throws InterruptedException {
        Thread.sleep(200);
        final RedisData redisData = RedisData.builder()
            .data(shop)
            .expireTime(LocalDateTime.now().plusSeconds(seconds))
            .build();
        final String jsonStr = JSONUtil.toJsonStr(redisData);
        stringRedisTemplate.opsForValue().set(CACHE_SHOP_KEY + id, jsonStr);
    }

    // 解决缓存穿透和击穿
    private Shop queryWithMutex(long id) throws JsonProcessingException {
        final ObjectMapper objectMapper = new ObjectMapper();
        // 从redis中查询是否存在当前商品详情的id
        // 设置key
        final String shopKey = CACHE_SHOP_KEY + id;
        final String redisValue = stringRedisTemplate.opsForValue().get(shopKey);
        // 此处有可能存在redisValue为空 那是为了应付穿透的情况 因此缓存空值
        if (StrUtil.isNotEmpty(redisValue)) {
            return JSONUtil.toBean(redisValue, Shop.class);
        }
        // 如果当前
        if (redisValue != null) {
            return null;
        }

        // 设置店铺锁的id
        final String lockId = "lock" + id;
        final Shop shop;
        try {
            // 当前未命中缓存,尝试获取锁,然后判断是否需要休眠并且重新获取锁
            final boolean tryLock = tryLock(lockId);
            // 如果没有获取到锁
            if (!tryLock) {
                Thread.sleep(50);
                this.queryWithMutex(id);
            }
            // 获取锁之后判断redis中是否有缓存,如果没有缓存则重建缓存
            final String secondRedisValue = stringRedisTemplate.opsForValue().get(shopKey);
            if (StrUtil.isNotEmpty(secondRedisValue)) {
                return objectMapper.readValue(secondRedisValue,Shop.class);
            }
            // 如果当前缓存不为空 说明还有可能进入穿透
            if (redisValue != null) {
                return null;
            }
            // 未命中,重建缓存
            Thread.sleep(50);
            shop = getById(id);
            if (shop == null) {
                //4：没有
                stringRedisTemplate.opsForValue().set(shopKey, "", CACHE_SHOP_TTL, TimeUnit.MINUTES);
                return null;
            }

            //5：有、将店铺数据写入redis中
            final String string = objectMapper.writeValueAsString(shop);
            stringRedisTemplate.opsForValue().set(shopKey , string, CACHE_SHOP_TTL, TimeUnit.MINUTES);
        } catch (InterruptedException e) {
            throw new RuntimeException();
        } finally {
            deleteLock(lockId);
        }
        return shop;
    }


    private boolean tryLock(final String key) {
        final Boolean lockShop = stringRedisTemplate.opsForValue().setIfAbsent(key, "1");
        return BooleanUtil.isTrue(lockShop);
    }

    private void deleteLock(final String key){
        stringRedisTemplate.delete(key);
    }

    @Override
    @Transactional
    public Result update(Shop shop) {
        Long id = shop.getId();
        if (id == null) {
            return Result.fail("店铺id不能为空");
        }
        // 1.更新数据库
        updateById(shop);
        // 2.删除缓存
        stringRedisTemplate.delete(CACHE_SHOP_KEY + id);
        return Result.ok();
    }



    @Override
    public Result queryShopByType(Integer typeId, Integer current, Double x, Double y) {
        // 1.判断是否需要根据坐标查询
        if (x == null || y == null) {
            // 不需要坐标查询，按数据库查询
            Page<Shop> page = query()
                    .eq("type_id", typeId)
                    .page(new Page<>(current, SystemConstants.DEFAULT_PAGE_SIZE));
            // 返回数据
            return Result.ok(page.getRecords());
        }

        // 2.计算分页参数
        int from = (current - 1) * SystemConstants.DEFAULT_PAGE_SIZE;
        int end = current * SystemConstants.DEFAULT_PAGE_SIZE;

        // 3.查询redis、按照距离排序、分页。结果：shopId、distance
        String key = SHOP_GEO_KEY + typeId;
        GeoResults<RedisGeoCommands.GeoLocation<String>> results = stringRedisTemplate.opsForGeo() // GEOSEARCH key BYLONLAT x y BYRADIUS 10 WITHDISTANCE
                .search(
                        key,
                        GeoReference.fromCoordinate(x, y),
                        new Distance(5000),
                        RedisGeoCommands.GeoSearchCommandArgs.newGeoSearchArgs().includeDistance().limit(end)
                );
        // 4.解析出id
        if (results == null) {
            return Result.ok(Collections.emptyList());
        }
        List<GeoResult<RedisGeoCommands.GeoLocation<String>>> list = results.getContent();
        if (list.size() <= from) {
            // 没有下一页了，结束
            return Result.ok(Collections.emptyList());
        }
        // 4.1.截取 from ~ end的部分
        List<Long> ids = new ArrayList<>(list.size());
        Map<String, Distance> distanceMap = new HashMap<>(list.size());
        list.stream().skip(from).forEach(result -> {
            // 4.2.获取店铺id
            String shopIdStr = result.getContent().getName();
            ids.add(Long.valueOf(shopIdStr));
            // 4.3.获取距离
            Distance distance = result.getDistance();
            distanceMap.put(shopIdStr, distance);
        });
        // 5.根据id查询Shop
        String idStr = StrUtil.join(",", ids);
        List<Shop> shops = query().in("id", ids).last("ORDER BY FIELD(id," + idStr + ")").list();
        for (Shop shop : shops) {
            shop.setDistance(distanceMap.get(shop.getId().toString()).getValue());
        }
        // 6.返回
        return Result.ok(shops);
    }
}
