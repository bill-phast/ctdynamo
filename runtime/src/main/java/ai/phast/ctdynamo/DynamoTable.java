package ai.phast.ctdynamo;

import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;

import java.util.Map;

public abstract class DynamoTable<T, PartitionT, SortT> extends DynamoIndex<T> {

    public abstract PartitionT getPartitionKey(T value);

    public abstract SortT getSortKey(T value);

    protected abstract Map<String, AttributeValue> keysToMap(PartitionT partitionValue, SortT sortValue);

    public final T get(T value) {
        return get(getPartitionKey(value), getSortKey(value));
    }

    public abstract T get(PartitionT partitionValue, SortT sortValue);

    public abstract void put(T value);

    public final void delete(T value) {
        delete(getPartitionKey(value), getSortKey(value));
    }

    public abstract void delete(PartitionT partitionKey, SortT sortKey);
}
