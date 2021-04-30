package ai.phast.ctdynamo;

import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

public abstract class DynamoCodec<T> {

    /**
     * If we don't suppress nulls, then we'll need a lot null attribute values. Since they are all identical and
     * immutable we may as well build one at initialiation time and be done.
     */
    public static final AttributeValue NULL_ATTRIBUTE_VALUE = AttributeValue.builder().nul(true).build();

    public abstract AttributeValue encode(T value);

    public abstract T decode(AttributeValue dynamoValue);
}
