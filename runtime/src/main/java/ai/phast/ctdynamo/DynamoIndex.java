package ai.phast.ctdynamo;

import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

import java.util.Map;

public abstract class DynamoIndex<T> {

    public abstract Map<String, AttributeValue> getExclusiveStart(T value);

}
