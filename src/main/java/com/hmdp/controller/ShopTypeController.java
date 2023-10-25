package com.hmdp.controller;

import cn.hutool.core.collection.CollUtil;
import cn.hutool.core.util.StrUtil;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.hmdp.dto.Result;
import com.hmdp.entity.ShopType;
import com.hmdp.service.IShopTypeService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import javax.annotation.Resource;
import java.util.List;

import static com.hmdp.utils.RedisConstants.SHOP_CATEGORY_KEY;

/**
 * <p>
 * 前端控制器
 * </p>
 *
 * @author 虎哥
 */
@RestController
@RequestMapping("/shop-type")
public class ShopTypeController {
    @Resource
    private IShopTypeService typeService;

    @Autowired
    private StringRedisTemplate stringRedisTemplate;

    @GetMapping("list")
    public Result queryTypeList() throws JsonProcessingException {
        //1: 从redis中查询是否有店铺列表信息
        final ObjectMapper objectMapper = new ObjectMapper();
        String cacheShop = stringRedisTemplate.opsForValue().get(SHOP_CATEGORY_KEY);

        if (StrUtil.isNotEmpty(cacheShop)) {
            //2: 有直接返回
            final List<ShopType> shopTypeList = objectMapper.readValue(cacheShop, new TypeReference<List<ShopType>>() {});
            return Result.ok(shopTypeList);
        }

        //3：没有、去数据库中查询
        List<ShopType> typeList = typeService.query().orderByAsc("sort").list();
        if (CollUtil.isEmpty(typeList)) {
            //4：没有
            return Result.fail("没有商品类别");
        }

        //5：有、将店铺数据写入redis中
        final String string = objectMapper.writeValueAsString(typeList);
        stringRedisTemplate.opsForValue().set(SHOP_CATEGORY_KEY , string);

        //6：返回店铺列表数据
        return Result.ok(typeList);
    }

}
