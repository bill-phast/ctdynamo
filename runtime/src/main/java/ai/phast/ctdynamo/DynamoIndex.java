package ai.phast.ctdynamo;

import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

import java.util.Map;

public abstract class DynamoIndex<T> extends DynamoCodec<T> {

    private final String tableName;

    public DynamoIndex(String tableName) {
        this.tableName = tableName;
    }

    public final String getTableName() {
        return tableName;
    }

    @Override
    public final AttributeValue encode(T value) {
        return AttributeValue.builder().m(encodeToMap(value)).build();
    }

    public abstract Map<String, AttributeValue> encodeToMap(T value);

    @Override
    public final T decode(AttributeValue dynamoValue) {
        return decode(dynamoValue.m());
    }

    public abstract T decode(Map<String, AttributeValue> map);

    public abstract Map<String, AttributeValue> getExclusiveStart(T value);

}
