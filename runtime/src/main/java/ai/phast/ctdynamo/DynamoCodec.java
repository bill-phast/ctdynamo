package ai.phast.ctdynamo;

import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

public abstract class DynamoCodec<T> {

    public abstract AttributeValue encode(T value);

    public abstract T decode(AttributeValue dynamoValue);
}
