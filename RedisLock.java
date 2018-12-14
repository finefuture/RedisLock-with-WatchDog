package com.houbank.framework.util.lock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.data.redis.connection.RedisConnection;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.util.concurrent.TimeUnit;

@Component
public class RedisLock {

    private static final Logger log = LoggerFactory.getLogger(RedisLock.class);

    @Resource
    private StringRedisTemplate redisTemplate;

    public Long lock (String lockKey, long millSeconds) {
        log.info("请求加锁");
        Long expireTime = currtTimeForRedis() + millSeconds + 3;
        String expireTimes = expireTime.toString();
        if (redisTemplate.opsForValue().setIfAbsent(lockKey, expireTimes)) {
            redisTemplate.expire(lockKey, millSeconds, TimeUnit.MILLISECONDS);
            log.info("获取锁成功 setNX");
            return expireTime;
        } else {
            String oldExpireTime = redisTemplate.opsForValue().get(lockKey);
            if (oldExpireTime != null && currtTimeForRedis().compareTo(Long.parseLong(oldExpireTime)) > 0) {
                String currentExpireTime = redisTemplate.opsForValue().getAndSet(lockKey, expireTimes);
                if (currentExpireTime != null && currentExpireTime.equals(oldExpireTime)) {
                    redisTemplate.expire(lockKey, millSeconds, TimeUnit.MILLISECONDS);
                    log.info("获取锁成功 getSet");
                    return expireTime;
                }
            }
        }
        log.error("获取锁失败");
        return -1L;
    }
    
    public void unlock (String lockKey, Long lockValue) {
        log.info("请求解锁");
        String redisValue = redisTemplate.opsForValue().get(lockKey);
        if (redisValue == null)
            return;

        if (Long.valueOf(redisValue).equals(lockValue)) {
            redisTemplate.delete(lockKey);
            log.info("解锁成功");
        }
    }

    public boolean delayTime (String lockKey, long millSeconds, int delayTimes, long lockValue) {
        String oldExpireTime = redisTemplate.opsForValue().get(lockKey);
        if (oldExpireTime == null) {
            log.error("业务持有锁延时失败, 过期时间为null, 可能key被误删");
            return false;
        }

        if (!Long.valueOf(oldExpireTime).equals(lockValue)) {
            log.error("业务持有锁延时失败, 锁被其他线程占有或key被设置了其他值");
            return false;
        }

        long newLockValue = lockValue + millSeconds*delayTimes;
        Long pttl = redisTemplate.getExpire(lockKey, TimeUnit.MILLISECONDS);
        redisTemplate.opsForValue().set(lockKey, Long.toString(newLockValue));
        redisTemplate.expire(lockKey, pttl + millSeconds, TimeUnit.MILLISECONDS);
        return true;
    }

    private Long currtTimeForRedis() {
        return redisTemplate.execute(RedisConnection :: time);
    }
}
