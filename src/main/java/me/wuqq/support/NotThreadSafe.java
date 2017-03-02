package me.wuqq.support;

import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;

@Retention(value = RetentionPolicy.CLASS)
public @interface NotThreadSafe {
}
