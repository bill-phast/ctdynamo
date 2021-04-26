package ai.phast.ctdynamo;

import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;

import java.util.Map;

public abstract class DynamoTable<T, PartitionT, SortT> extends DynamoIndex<T> {

    public final T get(T value) {
        return get(getPartitionKey(value), getSortKey(value));
    }

    public abstract PartitionT getPartitionKey(T value);

    public abstract SortT getSortKey(T value);

    protected abstract Map<String, AttributeValue> keysToMap(PartitionT partitionValue, SortT sortValue);

    private final DynamoDbClient client;

    private final String tableName;

    public DynamoTable(DynamoDbClient client, String tableName) {
        this.client = client;
        this.tableName = tableName;
    }

    public T get(PartitionT partitionValue, SortT sortValue) {
        var result = client.getItem(GetItemRequest.builder()
                                        .tableName(tableName)
                                        .key(keysToMap(partitionValue, sortValue))
                                        .build());
        return decode(result.item());
    }

    public void put(T value) {
        client.putItem(PutItemRequest.builder()
                           .tableName(tableName)
                           .item(encode(value))
                           .build());
    }

    public final void delete(T value) {
        delete(getPartitionKey(value), getSortKey(value));
    }

    public void delete(PartitionT partitionKey, SortT sortKey) {
        throw new UnsupportedOperationException();
    }
}
