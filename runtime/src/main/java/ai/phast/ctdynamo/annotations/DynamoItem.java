package ai.phast.ctdynamo.annotations;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

@Target(ElementType.TYPE) @Retention(RetentionPolicy.SOURCE)
public @interface DynamoItem {

    enum Output {
        TABLE, CODEC
    }

    Output[] value() default {Output.TABLE};

    boolean ignoreNulls() default true;

}
