package com.houbank.framework.util.lock;

import java.lang.annotation.Documented;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface Lock {

    String lockKey();

    long expireTime();//毫秒

    int delayTimes() default 0;

    byte delayWight() default 90; //百分比分子
}
