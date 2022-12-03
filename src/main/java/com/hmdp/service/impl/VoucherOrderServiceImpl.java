package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import com.hmdp.dto.Result;
import com.hmdp.entity.VoucherOrder;
import com.hmdp.mapper.VoucherOrderMapper;
import com.hmdp.service.ISeckillVoucherService;
import com.hmdp.service.IVoucherOrderService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.utils.RedisIdWorker;
import com.hmdp.utils.UserHolder;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.springframework.aop.framework.AopContext;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.connection.stream.*;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
public class VoucherOrderServiceImpl extends ServiceImpl<VoucherOrderMapper, VoucherOrder> implements IVoucherOrderService {

    @Resource
    private ISeckillVoucherService seckillVoucherService;

    @Resource
    private RedisIdWorker redisIdWorker;

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    @Resource
    private RedissonClient redissonClient;

    private static final DefaultRedisScript<Long> SECKILL_SCRIPT;
    static {
        SECKILL_SCRIPT = new DefaultRedisScript<>();
        SECKILL_SCRIPT.setLocation(new ClassPathResource("seckill.lua"));
        SECKILL_SCRIPT.setResultType(Long.class);
    }
//    private BlockingQueue<VoucherOrder> orderTasks = new ArrayBlockingQueue<>(1024 *1024);
    private static final ExecutorService SECKILL_ORDER_EXECUTOR = Executors.newSingleThreadExecutor();

    @PostConstruct
    private void init(){
        SECKILL_ORDER_EXECUTOR.submit(new VoucherOrderHandler());
    }

    private class VoucherOrderHandler implements Runnable{
        String queue = "stream.orders";
        @Override
        public void run() {
            while (true){
                try {
//                                    获取订单信息
//                    VoucherOrder voucherOrder = orderTasks.take();

//                    获取消息队列中订单信息  XREADGROUP GROUP g1 c1 COUNT 1 BLOCK 2000 STREAMS stream.orders >
                    List<MapRecord<String, Object, Object>> list = stringRedisTemplate.opsForStream().read(
                            Consumer.from("g1", "c1"),
                            StreamReadOptions.empty().count(1).block(Duration.ofSeconds(2)),
                            StreamOffset.create(queue, ReadOffset.lastConsumed())
                    );
//                    判断是否获取成功
                    if (list == null || list.isEmpty()){
//                    如果没消息则说明没有消息，继续下次循环
                        continue;
                    }
//                    如果获取成功则解析列表信息并下单
                    MapRecord<String, Object, Object> record = list.get(0);
                    Map<Object, Object> value = record.getValue();
                    VoucherOrder voucherOrder = BeanUtil.fillBeanWithMap(value, new VoucherOrder(), true);
//                    下单
                    handleVoucherOrder(voucherOrder);
//                    ACK确认 SACK stream.orders g1 id
                    stringRedisTemplate.opsForStream().acknowledge(queue,"g1",record.getId());

                } catch (Exception e) {
                    log.error("订单处理异常",e);
                    handlePandingList();
                }
            }
        }

        private void handlePandingList() {
            while (true){
                try {
//                                    获取订单信息
//                    VoucherOrder voucherOrder = orderTasks.take();

//                    获取panding-List中订单信息  XREADGROUP GROUP g1 c1 COUNT 1 STREAMS stream.orders 0
                    List<MapRecord<String, Object, Object>> list = stringRedisTemplate.opsForStream().read(
                            Consumer.from("g1", "c1"),
                            StreamReadOptions.empty().count(1),
                            StreamOffset.create(queue, ReadOffset.from("0"))
                    );
//                    判断是否获取成功
                    if (list == null || list.isEmpty()){
//                    如果没消息则说明panding-List没有消息，结束循环
                        break;
                    }
//                    如果获取成功则解析列表信息并下单
                    MapRecord<String, Object, Object> record = list.get(0);
                    Map<Object, Object> value = record.getValue();
                    VoucherOrder voucherOrder = BeanUtil.fillBeanWithMap(value, new VoucherOrder(), true);
//                    下单
                    handleVoucherOrder(voucherOrder);
//                    ACK确认 SACK stream.orders g1 id
                    stringRedisTemplate.opsForStream().acknowledge(queue,"g1",record.getId());

                } catch (Exception e) {
                    log.error("panding-List订单处理异常",e);
                    try {
                        Thread.sleep(20);
                    } catch (InterruptedException ex) {
                        ex.printStackTrace();
                    }
                }
            }
        }
    }

    private void handleVoucherOrder(VoucherOrder voucherOrder) {
//        获取用户
        Long userId = voucherOrder.getUserId();
//        创建锁对象
        RLock lock = redissonClient.getLock("lock:order:" + userId);
//        获取锁
        boolean isLock = lock.tryLock();
        if (!isLock){
            log.error("不允许重复下单");
            return;
        }
        try {
            //            获取代理对象（事务）
            proxy.createVoucherOrder(voucherOrder);
        }finally {
//            释放锁
            lock.unlock();
        }
    }

    private IVoucherOrderService proxy;

    @Override
    public Result seckillVoucher(Long voucherId) {
//        获取用户
        Long id = UserHolder.getUser().getId();
//        获取订单id
        long orderId = redisIdWorker.nextId("order");
//      执行lua脚本
        Long result = stringRedisTemplate.execute(
                SECKILL_SCRIPT,
                Collections.emptyList(),
                voucherId.toString(), id.toString(), String.valueOf(orderId)
        );
//      判断结果是0
        int r = result.intValue();
        if (result != 0){
            //      不为0 没有购买资格
            return Result.fail(result == 1 ? "库存不足" : "不能重复下单");
        }

//      为0 把下单信息保存到阻塞队列
//        // 创建订单
//        VoucherOrder voucherOrder = new VoucherOrder();
//        // 订单id
//        voucherOrder.setId(orderId);
//        // 用户id
//        Long userId = UserHolder.getUser().getId();
//        voucherOrder.setUserId(userId);
//        // 代金券id
//        voucherOrder.setVoucherId(voucherId);
////        创建阻塞队列
//        orderTasks.add(voucherOrder);
        //            获取代理对象（事务）
        proxy = (IVoucherOrderService) AopContext.currentProxy();

//      返回订单id
        return Result.ok(0);


//        // 1.查询优惠券
//        SeckillVoucher voucher = seckillVoucherService.getById(voucherId);
//        // 2.判断秒杀是否开始
//        if (voucher.getBeginTime().isAfter(LocalDateTime.now())) {
//            // 尚未开始
//            return Result.fail("秒杀尚未开始！");
//        }
//        // 3.判断秒杀是否已经结束
//        if (voucher.getEndTime().isBefore(LocalDateTime.now())) {
//            // 尚未开始
//            return Result.fail("秒杀已经结束！");
//        }
//        // 4.判断库存是否充足
//        if (voucher.getStock() < 1) {
//            // 库存不足
//            return Result.fail("库存不足！");
//        }
//
//        Long id = UserHolder.getUser().getId();
//
////        创建锁对象
////        SimpleRedisLock lock = new SimpleRedisLock("order:" + id, stringRedisTemplate);
//        RLock lock = redissonClient.getLock("lock:order:" + id);
////        获取锁
//        boolean isLock = lock.tryLock();
//
//        if (!isLock){
//            return Result.fail("不允许重复下单");
//        }
//
//        try {
//            //            获取代理对象（事务）
//            IVoucherOrderService proxy = (IVoucherOrderService) AopContext.currentProxy();
//            return proxy.createVoucherOrder(voucherId);
//        }finally {
////            释放锁
//            lock.unlock();
//        }


    }

    @Override
    @Transactional
    public void createVoucherOrder(VoucherOrder voucherOrder) {
        //      一人一单
        Long id = voucherOrder.getUserId();

        Integer count = query().eq("user_id", id).eq("voucher_id", voucherOrder.getVoucherId()).count();
        if (count > 0) {
            log.error("用户已经买过一次了");
            return;
        }
        //5，扣减库存
        boolean success = seckillVoucherService.update()
                .setSql("stock= stock -1")
                .eq("voucher_id", voucherOrder.getVoucherId())
                .gt("stock", 0)
                .update();
        if (!success) {
            log.error("库存不足！");
            return;
        }

        save(voucherOrder);
    }
}
