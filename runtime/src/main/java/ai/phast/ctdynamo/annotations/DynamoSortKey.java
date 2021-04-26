package ai.phast.ctdynamo.annotations;

import ai.phast.ctdynamo.DynamoCodec;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Target({ElementType.FIELD, ElementType.METHOD}) @Retention(RetentionPolicy.SOURCE)
public @interface DynamoSortKey {
    String value() default "";

    Class<? extends DynamoCodec<?, ?>> codec() default DefaultCodec.class;
}
