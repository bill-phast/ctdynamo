package ai.phast.ctdynamo;

import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

import java.util.Map;

public abstract class DynamoIndex<T> extends DynamoCodec<T, Map<String, AttributeValue>> {

    public abstract Map<String, AttributeValue> getExclusiveStart(T value);

}
