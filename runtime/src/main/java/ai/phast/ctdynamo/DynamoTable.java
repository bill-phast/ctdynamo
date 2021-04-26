package ai.phast.ctdynamo;

import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.DeleteItemRequest;
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

public abstract class DynamoTable<T, PartitionT, SortT> extends DynamoIndex<T, PartitionT, SortT> {

    public DynamoTable(DynamoDbClient client, DynamoDbAsyncClient asyncClient, String tableName,
                       String partitionKeyAttribute, String sortKeyAttribute) {
        super(client, asyncClient, tableName, null, partitionKeyAttribute, sortKeyAttribute);
    }

    public final T get(T value) {
        return get(getPartitionKey(value), getSortKey(value));
    }

    public T get(PartitionT partitionValue, SortT sortValue) {
        var request = GetItemRequest.builder()
                          .tableName(getTableName())
                          .key(keysToMap(partitionValue, sortValue))
                          .build();
        var response = (getClient() == null
                        ? getAsyncClient().getItem(request).join()
                        : getClient().getItem(request));
        return (response == null ? null : decode(response.item()));
    }

    public final CompletableFuture<T> getAsync(T value) {
        return getAsync(getPartitionKey(value), getSortKey(value));
    }

    public CompletableFuture<T> getAsync(PartitionT partitionValue, SortT sortValue) {
        var request = GetItemRequest.builder()
                          .tableName(getTableName())
                          .key(keysToMap(partitionValue, sortValue))
                          .build();
        var response = (getAsyncClient() == null
                        ? CompletableFuture.supplyAsync(() -> getClient().getItem(request))
                        : getAsyncClient().getItem(request));
        return response.thenApply(r -> r.item() == null ? null : decode(r.item()));
    }

    public void put(T value) {
        var request = PutItemRequest.builder()
            .tableName(getTableName())
            .item(encodeToMap(value))
            .build();
        if (getClient() == null) {
            getAsyncClient().putItem(request).join();
        } else {
            getClient().putItem(request);
        }
    }

    public CompletableFuture<Void> putAsync(T value) {
        var request = PutItemRequest.builder()
                          .tableName(getTableName())
                          .item(encodeToMap(value))
                          .build();
        return (getAsyncClient() == null
                ? CompletableFuture.runAsync(() -> getClient().putItem(request))
                : getAsyncClient().putItem(request).thenApply(r -> null));
    }

    public final void delete(T value) {
        delete(getPartitionKey(value), getSortKey(value));
    }

    public void delete(PartitionT partitionKey, SortT sortKey) {
        var request = DeleteItemRequest.builder()
                          .tableName(getTableName())
                          .key(keysToMap(partitionKey, sortKey))
                          .build();
        if (getClient() == null) {
            getAsyncClient().deleteItem(request).join();
        } else {
            getClient().deleteItem(request);
        }
    }

    public final CompletableFuture<Void> deleteAsync(T value) {
        return deleteAsync(getPartitionKey(value), getSortKey(value));
    }

    public CompletableFuture<Void> deleteAsync(PartitionT partitionKey, SortT sortKey) {
        var request = DeleteItemRequest.builder()
                          .tableName(getTableName())
                          .key(keysToMap(partitionKey, sortKey))
                          .build();
        return (getAsyncClient() == null
                ? CompletableFuture.runAsync(() -> getClient().deleteItem(request))
                : getAsyncClient().deleteItem(request).thenApply(r -> null));
    }

    public abstract PartitionT getPartitionKey(T value);

    public abstract SortT getSortKey(T value);

    /**
     * Convert the partition and sort keys to a map.
     * @param partitionValue The value of the partition key
     * @param sortValue The value of the sort key. Must be null if there is no sort key
     * @return A dynamo-friendly map of attribute values
     */
    protected final Map<String, AttributeValue> keysToMap(PartitionT partitionValue, SortT sortValue) {
        return sortValue == null
               ? Collections.singletonMap(getPartitionKeyAttribute(), partitionValueToAttributeValue(partitionValue))
               : Map.of(getPartitionKeyAttribute(), partitionValueToAttributeValue(partitionValue),
                   getSortKeyAttribute(), sortValueToAttributeValue(sortValue));
    }
}
