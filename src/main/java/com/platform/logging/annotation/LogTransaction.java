package com.platform.logging.annotation;

import com.platform.logging.model.EventType;
import java.lang.annotation.*;

@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
public @interface LogTransaction {
    String    serviceName() default "";
    String    apiName()     default "";
    EventType eventType()  default EventType.API_CALL;
}
