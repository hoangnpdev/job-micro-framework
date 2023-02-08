package nph.aspect;

import nph.driver.YarnConfig;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
public @interface JobConf {
    public YarnConfig value() default YarnConfig.DEV_MEMORY;
}
