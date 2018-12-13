package com.houbank.framework.util.lock;

import com.houbank.framework.util.LockException;
import io.netty.util.HashedWheelTimer;
import io.netty.util.Timeout;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.reflect.MethodSignature;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.stereotype.Component;

import java.lang.reflect.Method;
import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

@Component
@Aspect
public class WatchDog {

    private static final Logger log = LoggerFactory.getLogger(WatchDog.class);

    @Autowired
    private RedisLock redisLock;

    @Autowired
    private ThreadPoolTaskExecutor executor;

    static final HashedWheelTimer hashedWheelTimer = new HashedWheelTimer(100, TimeUnit.MILLISECONDS);

    @Around("@annotation(Lock)")
    public Object proxy (ProceedingJoinPoint joinPoint) throws Throwable {
        MethodSignature signature = (MethodSignature) joinPoint.getSignature();
        Method method = signature.getMethod();
        Lock lock = method.getAnnotation(Lock.class);
        Long lockValue = redisLock.lock(lock.lockKey(), lock.expireTime());
        if (lockValue == -1)
            throw new LockException();

        Future<Object> future = executor.submit(() -> {//将真正的任务执行逻辑放入线程池,返回Future
            try {
                return joinPoint.proceed();
            } catch (Throwable throwable) {
                log.error("任务执行错误, 异常:{}", throwable);
                throwable.printStackTrace();
                throw new RuntimeException("任务执行错误");
            } finally {
                redisLock.unlock(lock.lockKey(), lockValue);
            }
        });
        Queue<Timeout> delayTaskQueue = watch(future, lock, lockValue);
        while (!future.isDone()) ;
        executor.execute(() -> {//任务完成之后遍历延长任务队列,将取消的定时任务取消,放在其他线程执行
            while (!delayTaskQueue.isEmpty()) {
                Timeout timeout = delayTaskQueue.poll();
                timeout.cancel();//将能取消的定时任务取消
            }
        });
        return future.get();
    }

    public Queue<Timeout> watch (Future<Object> future, Lock lock, long lockValue) {
        Queue<Timeout> delayTaskQueue = new LinkedList<>();
        long expireTime = lock.expireTime();
        int delayTimes = lock.delayTimes();
        byte delayWight = lock.delayWight();
        while (delayTimes > 0) {
            int finalDelayTimes = delayTimes;
            //根据注解设置的延长次数将延时任务注册到时间轮中,注册规则:次数*超时时间*9/10(默认9/10),次数依次递减
            delayTaskQueue.offer(hashedWheelTimer.newTimeout(timeout -> {
                if (timeout.isCancelled() || future.isDone()) //如果定时任务已取消或业务任务已完成不进行操作
                    return;

                //锁的旧值,当finalDelayTimes=1时,也就是第一次延时的时候,旧值为lockValue,延时之后新值为 lockValue + expireTime
                //当finalDelayTimes=2时,也就是第二次延时的时候,旧值为lockValue + expireTime ,也就是第一次延时之后设置的值，
                //以此类推
                long oldLockValue = lockValue + (finalDelayTimes - 1)*expireTime;
                //如果任务未完成就延长锁时间
                boolean delayResult = redisLock.delayTime(lock.lockKey(), expireTime, finalDelayTimes, oldLockValue);
                if (!delayResult) {
                    //延时失败,可能原因:因为业务超时被其他锁占用或者值被改变,或者值为null,这种情况将正在执行的任务取消
                    //如果出现这种情况,请确认是否有对锁的key做过set,以及调整@Lock里面delayWight的值以确保在锁超时之前完成延时的动作
                    future.cancel(true);
                    log.error("延时失败, 取消任务");
                }
            }, (expireTime * delayWight * finalDelayTimes) / 100, TimeUnit.MILLISECONDS));
            delayTimes--;
        }
        return delayTaskQueue;
    }
}
