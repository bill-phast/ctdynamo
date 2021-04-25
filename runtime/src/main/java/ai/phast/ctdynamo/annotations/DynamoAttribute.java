package ai.phast.ctdynamo.annotations;

import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.util.function.Function;

@Target({ElementType.TYPE, ElementType.FIELD, ElementType.METHOD}) @Retention(RetentionPolicy.SOURCE)
public @interface DynamoAttribute {

    String value() default "";

    Class<?> encodeWith() default Void.class;
}
