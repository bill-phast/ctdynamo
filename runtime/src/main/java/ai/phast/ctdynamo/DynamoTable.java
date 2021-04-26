package ai.phast.ctdynamo;

import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.DeleteItemRequest;
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;

import java.util.Map;
import java.util.concurrent.CompletableFuture;

public abstract class DynamoTable<T, PartitionT, SortT> extends DynamoIndex<T> {

    private final DynamoDbClient client;

    private final DynamoDbAsyncClient asyncClient;

    public DynamoTable(DynamoDbClient client, DynamoDbAsyncClient asyncClient, String tableName) {
        super(tableName);
        this.client = client;
        this.asyncClient = asyncClient;
    }

    public final T get(T value) {
        return get(getPartitionKey(value), getSortKey(value));
    }

    public T get(PartitionT partitionValue, SortT sortValue) {
        var request = GetItemRequest.builder()
                          .tableName(getTableName())
                          .key(keysToMap(partitionValue, sortValue))
                          .build();
        var response = (client == null
                        ? asyncClient.getItem(request).join()
                        : client.getItem(request));
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
        var response = (asyncClient == null
                        ? CompletableFuture.supplyAsync(() -> client.getItem(request))
                        : asyncClient.getItem(request));
        return response.thenApply(r -> r.item() == null ? null : decode(r.item()));
    }

    public void put(T value) {
        var request = PutItemRequest.builder()
            .tableName(getTableName())
            .item(encodeToMap(value))
            .build();
        if (client == null) {
            asyncClient.putItem(request).join();
        } else {
            client.putItem(request);
        }
    }

    public CompletableFuture<Void> putAsync(T value) {
        var request = PutItemRequest.builder()
                          .tableName(getTableName())
                          .item(encodeToMap(value))
                          .build();
        return (asyncClient == null
                ? CompletableFuture.runAsync(() -> client.putItem(request))
                : asyncClient.putItem(request).thenApply(r -> null));
    }

    public final void delete(T value) {
        delete(getPartitionKey(value), getSortKey(value));
    }

    public void delete(PartitionT partitionKey, SortT sortKey) {
        var request = DeleteItemRequest.builder()
                          .tableName(getTableName())
                          .key(keysToMap(partitionKey, sortKey))
                          .build();
        if (client == null) {
            asyncClient.deleteItem(request).join();
        } else {
            client.deleteItem(request);
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
        return (asyncClient == null
                ? CompletableFuture.runAsync(() -> client.deleteItem(request))
                : asyncClient.deleteItem(request).thenApply(r -> null));
    }

    public abstract PartitionT getPartitionKey(T value);

    public abstract SortT getSortKey(T value);

    /**
     * Convert the partition and sort keys to a map.
     * @param partitionValue The value of the partition key
     * @param sortValue The value of the sort key. Must be null if there is no sort key
     * @return A dynamo-friendly map of attribute values
     */
    protected abstract Map<String, AttributeValue> keysToMap(PartitionT partitionValue, SortT sortValue);
}
