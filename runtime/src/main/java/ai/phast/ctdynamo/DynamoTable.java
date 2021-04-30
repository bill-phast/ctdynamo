package ai.phast.ctdynamo;

import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.ConsumedCapacity;
import software.amazon.awssdk.services.dynamodb.model.DeleteItemRequest;
import software.amazon.awssdk.services.dynamodb.model.DeleteItemResponse;
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.GetItemResponse;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.dynamodb.model.PutItemResponse;
import software.amazon.awssdk.services.dynamodb.model.ReturnConsumedCapacity;

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
        var response = get(GetItemRequest.builder()
                               .tableName(getTableName())
                               .key(keysToMap(partitionValue, sortValue))
                               .build());
        return response.hasItem() ? decode(response.item()) : null;
    }

    public final ExtendedItemResult<T> getExtended(T value, boolean useConsistentRead) {
        return getExtended(getPartitionKey(value), getSortKey(value), useConsistentRead);
    }

    public ExtendedItemResult<T> getExtended(PartitionT partitionValue, SortT sortValue, boolean useConsistentRead) {
        var response = get(GetItemRequest.builder()
                               .tableName(getTableName())
                               .key(keysToMap(partitionValue, sortValue))
                               .returnConsumedCapacity(ReturnConsumedCapacity.INDEXES)
                               .consistentRead(useConsistentRead)
                               .build());
        return new ExtendedItemResult<>(response.hasItem() ? decode(response.item()) : null,
            response.consumedCapacity());
    }

    public final CompletableFuture<T> getAsync(T value) {
        return getAsync(getPartitionKey(value), getSortKey(value));
    }

    public CompletableFuture<T> getAsync(PartitionT partitionValue, SortT sortValue) {
        return getAsync(GetItemRequest.builder()
                            .tableName(getTableName())
                            .key(keysToMap(partitionValue, sortValue))
                            .build())
                   .thenApply(r -> r.hasItem() ? decode(r.item()) : null);
    }

    public final CompletableFuture<ExtendedItemResult<T>> getExtendedAsync(T value, boolean useConsistentRead) {
        return getExtendedAsync(getPartitionKey(value), getSortKey(value), useConsistentRead);
    }

    public CompletableFuture<ExtendedItemResult<T>> getExtendedAsync(PartitionT partitionValue, SortT sortValue, boolean useConsistentRead) {
        return getAsync(GetItemRequest.builder()
                            .tableName(getTableName())
                            .key(keysToMap(partitionValue, sortValue))
                            .returnConsumedCapacity(ReturnConsumedCapacity.INDEXES)
                            .consistentRead(useConsistentRead)
                            .build())
                   .thenApply(r -> new ExtendedItemResult<>(r.hasItem() ? decode(r.item()) : null, r.consumedCapacity()));
    }

    private GetItemResponse get(GetItemRequest request) {
        return getClient() == null ? getAsyncClient().getItem(request).join() : getClient().getItem(request);
    }

    private CompletableFuture<GetItemResponse> getAsync(GetItemRequest request) {
        return getAsyncClient() == null
               ? CompletableFuture.supplyAsync(() -> getClient().getItem(request))
               : getAsyncClient().getItem(request);
    }

    public void put(T value) {
        var request = PutItemRequest.builder()
            .tableName(getTableName())
            .item(encode(value))
            .build();
        if (getClient() == null) {
            getAsyncClient().putItem(request).join();
        } else {
            getClient().putItem(request);
        }
    }

    public ConsumedCapacity putExtended(T value) {
        var request = PutItemRequest.builder()
                          .tableName(getTableName())
                          .item(encode(value))
                          .returnConsumedCapacity(ReturnConsumedCapacity.INDEXES)
                          .build();
        return (getClient() == null
                ? getAsyncClient().putItem(request).join()
                : getClient().putItem(request)).consumedCapacity();
    }

    public CompletableFuture<Void> putAsync(T value) {
        var request = PutItemRequest.builder()
                          .tableName(getTableName())
                          .item(encode(value))
                          .build();
        return (getAsyncClient() == null
                ? CompletableFuture.runAsync(() -> getClient().putItem(request))
                : getAsyncClient().putItem(request).thenApply(r -> null));
    }

    public CompletableFuture<ConsumedCapacity> putExtendedAsync(T value) {
        var request = PutItemRequest.builder()
                          .tableName(getTableName())
                          .item(encode(value))
                          .returnConsumedCapacity(ReturnConsumedCapacity.INDEXES)
                          .build();
        return (getAsyncClient() == null
                ? CompletableFuture.supplyAsync(() -> getClient().putItem(request))
                : getAsyncClient().putItem(request)).thenApply(PutItemResponse::consumedCapacity);
    }

    public final void delete(T value) {
        delete(getPartitionKey(value), getSortKey(value));
    }

    public void delete(PartitionT partitionKey, SortT sortKey) {
        delete(DeleteItemRequest.builder()
                   .tableName(getTableName())
                   .key(keysToMap(partitionKey, sortKey))
                   .build());
    }

    public final ConsumedCapacity deleteExtended(T value) {
        return deleteExtended(getPartitionKey(value), getSortKey(value));
    }

    public ConsumedCapacity deleteExtended(PartitionT partitionKey, SortT sortKey) {
        return delete(DeleteItemRequest.builder()
                          .tableName(getTableName())
                          .key(keysToMap(partitionKey, sortKey))
                          .returnConsumedCapacity(ReturnConsumedCapacity.INDEXES)
                          .build()).consumedCapacity();
    }
    
    public final CompletableFuture<Void> deleteAsync(T value) {
        return deleteAsync(getPartitionKey(value), getSortKey(value));
    }

    public CompletableFuture<Void> deleteAsync(PartitionT partitionKey, SortT sortKey) {
        return deleteAsync(DeleteItemRequest.builder()
                          .tableName(getTableName())
                          .key(keysToMap(partitionKey, sortKey))
                          .build()).thenApply(r -> null);
    }

    public final CompletableFuture<ConsumedCapacity> deleteExtendedAsync(T value) {
        return deleteExtendedAsync(getPartitionKey(value), getSortKey(value));
    }

    public CompletableFuture<ConsumedCapacity> deleteExtendedAsync(PartitionT partitionKey, SortT sortKey) {
        return deleteAsync(DeleteItemRequest.builder()
                               .tableName(getTableName())
                               .key(keysToMap(partitionKey, sortKey))
                               .returnConsumedCapacity(ReturnConsumedCapacity.INDEXES)
                               .build())
                   .thenApply(DeleteItemResponse::consumedCapacity);
    }

    private DeleteItemResponse delete(DeleteItemRequest request) {
        return getClient() == null ? getAsyncClient().deleteItem(request).join() : getClient().deleteItem(request);
    }

    private CompletableFuture<DeleteItemResponse> deleteAsync(DeleteItemRequest request) {
        return getAsyncClient() == null ? CompletableFuture.supplyAsync(() -> getClient().deleteItem(request)) : getAsyncClient().deleteItem(request);
    }

    public abstract <SecondaryPartitionT, SecondarySortT> DynamoIndex<T, SecondaryPartitionT, SecondarySortT> getIndex(
        String name, Class<SecondaryPartitionT> secondaryPartitionClass, Class<SecondarySortT> secondarySortClass);

    public abstract Map<String, AttributeValue> encode(T value);

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
