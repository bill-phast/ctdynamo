package ai.phast.ctdynamo;

import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.dynamodb.model.DeleteItemRequest;
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;

import java.util.concurrent.CompletableFuture;

/**
 * A class that adds asynchronous calls to the DynamoTable class. The synchronous calls use the async calls, then
 * immediately join with the future returned.
 * @param <T>
 * @param <PartitionT>
 * @param <SortT>
 */
public abstract class DynamoAsyncTable<T, PartitionT, SortT> extends DynamoTable<T, PartitionT, SortT> {

    private final DynamoDbAsyncClient client;

    private final String tableName;

    public DynamoAsyncTable(DynamoDbAsyncClient client, String tableName) {
        this.client = client;
        this.tableName = tableName;
    }

    public final CompletableFuture<T> getAsync(T value) {
        return getAsync(getPartitionKey(value), getSortKey(value));
    }

    public CompletableFuture<T> getAsync(PartitionT partitionValue, SortT sortValue) {
        return client.getItem(GetItemRequest.builder()
                                  .tableName(tableName)
                                  .key(keysToMap(partitionValue, sortValue))
                                  .build())
                   .thenApply(response -> decode(response.item()));
    }

    @Override
    public T get(PartitionT partitionValue, SortT sortValue) {
        return getAsync(partitionValue, sortValue).join();
    }

    public CompletableFuture<Void> putAsync(T value) {
        return client.putItem(PutItemRequest.builder()
                                  .tableName(tableName)
                                  .item(encode(value))
                                  .build())
                   .thenApply(response -> null);
    }

    @Override
    public void put(T value) {
        putAsync(value).join();
    }

    public final CompletableFuture<Void> deleteAsync(T value) {
        return deleteAsync(getPartitionKey(value), getSortKey(value));
    }

    public CompletableFuture<Void> deleteAsync(PartitionT partitionKey, SortT sortKey) {
        return client.deleteItem(DeleteItemRequest.builder()
                                     .tableName(tableName)
                                     .key(keysToMap(partitionKey, sortKey))
                                     .build())
                   .thenApply(response -> null);
    }

    @Override
    public void delete(PartitionT partitionKey, SortT sortKey) {
        deleteAsync(partitionKey, sortKey).join();
    }
}
