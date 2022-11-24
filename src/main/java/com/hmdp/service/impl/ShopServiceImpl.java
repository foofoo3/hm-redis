package com.hmdp.service.impl;

import cn.hutool.core.util.BooleanUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONUtil;
import com.hmdp.dto.Result;
import com.hmdp.entity.Shop;
import com.hmdp.mapper.ShopMapper;
import com.hmdp.service.IShopService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;

import java.util.concurrent.TimeUnit;

import static com.hmdp.utils.RedisConstants.*;

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

    @Override
    public Result queryById(Long id) {
//        缓存穿透
//        Shop shop = queryWithPassThrough(id);

//        用互斥锁解决缓存击穿
        Shop shop = queryWithMutex(id);
        if (shop == null){
            return Result.fail("店铺不存在");
        }
//        返回
        return Result.ok(shop);
    }

    public Shop queryWithMutex(Long id){
        String key = CACHE_SHOP_KEY + id;
//        从redis查询缓存
        String shopJson = stringRedisTemplate.opsForValue().get(key);
//        判断是否存在
        if (StrUtil.isNotBlank(shopJson)){
//            存在 直接返回
            return JSONUtil.toBean(shopJson, Shop.class);
        }
//        判断redis是否为空值
        if (shopJson != null){
            return null;
        }
//         获取互斥锁
        String lockKey = LOCK_SHOP_KEY + id;
        Shop shop = null;
        try {
            boolean isLock = tryLock(lockKey);
//        判断是否获取成功
            if (!isLock){
    //            失败则过会重试
                Thread.sleep(50);
                return queryWithMutex(id);
            }
//        成功则查询数据库
            shop = getById(id);
//        不存在返回错误
            if (shop == null){
    //            redis写入空值
                stringRedisTemplate.opsForValue().set(key,"",CACHE_NULL_TTL, TimeUnit.MINUTES);
                return null;
            }
//        存在 写入redis
            stringRedisTemplate.opsForValue().set(key,JSONUtil.toJsonStr(shop),CACHE_SHOP_TTL, TimeUnit.MINUTES);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }finally {
            //        释放互斥锁
            unLock(lockKey);
        }
//        返回
        return shop;
    }

    public Shop queryWithPassThrough(Long id){
        String key = CACHE_SHOP_KEY + id;
//        从redis查询缓存
        String shopJson = stringRedisTemplate.opsForValue().get(key);
//        判断是否存在
        if (StrUtil.isNotBlank(shopJson)){
//            存在 直接返回
            return JSONUtil.toBean(shopJson, Shop.class);
        }
//        判断redis是否为空值
        if (shopJson != null){
            return null;
        }
//        不存在查询数据库
        Shop shop = getById(id);
//        不存在返回错误
        if (shop == null){
//            redis写入空值
            stringRedisTemplate.opsForValue().set(key,"",CACHE_NULL_TTL, TimeUnit.MINUTES);
            return null;
        }
//        存在 写入redis
        stringRedisTemplate.opsForValue().set(key,JSONUtil.toJsonStr(shop),CACHE_SHOP_TTL, TimeUnit.MINUTES);
//        返回
        return shop;
    }

    private boolean tryLock(String key){
        Boolean aBoolean = stringRedisTemplate.opsForValue().setIfAbsent(key, "1", 10, TimeUnit.SECONDS);
        return BooleanUtil.isTrue(aBoolean);
    }

    private void unLock(String key){
        stringRedisTemplate.delete(key);
    }

    @Override
    public Result update(Shop shop) {
        Long id = shop.getId();
        if (id == null){
            return Result.fail("店铺id不能为空");
        }
//        更新数据库
        updateById(shop);
//        删除缓存
        stringRedisTemplate.delete(CACHE_SHOP_KEY + id);
        return Result.ok();
    }
}
