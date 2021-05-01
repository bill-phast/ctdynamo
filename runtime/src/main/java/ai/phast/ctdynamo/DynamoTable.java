package ai.phast.ctdynamo;

import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.BatchGetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.BatchGetItemResponse;
import software.amazon.awssdk.services.dynamodb.model.ConsumedCapacity;
import software.amazon.awssdk.services.dynamodb.model.DeleteItemRequest;
import software.amazon.awssdk.services.dynamodb.model.DeleteItemResponse;
import software.amazon.awssdk.services.dynamodb.model.GetItemRequest;
import software.amazon.awssdk.services.dynamodb.model.GetItemResponse;
import software.amazon.awssdk.services.dynamodb.model.KeysAndAttributes;
import software.amazon.awssdk.services.dynamodb.model.PutItemRequest;
import software.amazon.awssdk.services.dynamodb.model.PutItemResponse;
import software.amazon.awssdk.services.dynamodb.model.ReturnConsumedCapacity;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiFunction;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public abstract class DynamoTable<T, PartitionT, SortT> extends DynamoIndex<T, PartitionT, SortT> {

    private static final int MAX_ITEMS_PER_BATCH = 25;

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

    private List<T> getBatch(List<T> keys) {
        return getBatch(keys.stream().map(this::getPartitionKey).collect(Collectors.toList()),
            keys.stream().map(this::getSortKey).collect(Collectors.toList()));
    }

    private List<T> getBatch(List<PartitionT> partitionKeys, List<SortT> sortKeys) {
        int numItems = partitionKeys.size();
        if (numItems != sortKeys.size()) {
            throw new IllegalArgumentException("Got " + numItems + " partition keys, and "
                                                   + sortKeys.size() + " sort keys. They must be equal");
        }
        var result = new ArrayList<T>(numItems);
        for (int i = 0; i < numItems; i += MAX_ITEMS_PER_BATCH) {
            var request = BatchGetItemRequest.builder().requestItems(buildBatchItems(partitionKeys, sortKeys, i)).build();
            var response = getClient() == null ? getAsyncClient().batchGetItem(request).join()
                                               : getClient().batchGetItem(request);
            response.responses().get(getTableName()).stream()
                .map(this::decode)
                .forEach(result::add);
        }
        return result;
    }

    private List<T> getBatchAsync(List<T> keys) {
        return getBatch(keys.stream().map(this::getPartitionKey).collect(Collectors.toList()),
            keys.stream().map(this::getSortKey).collect(Collectors.toList()));
    }

    private CompletableFuture<List<T>> getBatchAsync(List<PartitionT> partitionKeys, List<SortT> sortKeys) {
        int numItems = partitionKeys.size();
        if (numItems != sortKeys.size()) {
            throw new IllegalArgumentException("Got " + numItems + " partition keys, and "
                                                   + sortKeys.size() + " sort keys. They must be equal");
        }
        BiFunction<List<T>, BatchGetItemResponse, List<T>> merger = (list, response) -> {
            if (response.hasResponses()) {
                response.responses().get(getTableName()).forEach(m -> list.add(decode(m)));
            }
            return list;
        };
        CompletableFuture<List<T>> result = CompletableFuture.completedFuture(new ArrayList<>(numItems));
        for (int i = 0; i < numItems; i += MAX_ITEMS_PER_BATCH) {
            var request = BatchGetItemRequest.builder().requestItems(buildBatchItems(partitionKeys, sortKeys, i)).build();
            result = result.thenCombine(
                getAsyncClient() == null ? CompletableFuture.supplyAsync(() -> getClient().batchGetItem(request))
                                         : getAsyncClient().batchGetItem(request),
                merger);
        }
        return result;
    }

    /**
     * Build a map from table name to a list of keys. The result will take all items until either the end of the list or\
     * MAX_ITEMS_PER_BATCH has been reached. This is useful for many batch requests.
     * @param partitionKeys A list of partition keys
     * @param sortKeys A list of sort keys
     * @param offset Where in each list to start
     * @return A data structure that will hold as many key items as possible from the list
     */
    private Map<String, KeysAndAttributes> buildBatchItems(List<PartitionT> partitionKeys, List<SortT> sortKeys, int offset) {
        return Collections.singletonMap(getTableName(),
            KeysAndAttributes.builder()
                .keys(IntStream.range(offset, Math.min(partitionKeys.size(), offset + MAX_ITEMS_PER_BATCH) - 1)
                          .mapToObj(i -> keysToMap(partitionKeys.get(i), sortKeys.get(i)))
                          .collect(Collectors.toList()))
                .build());
    }

    public ExtendedItemResult<T> putExtended(T value) {
        var putResponse = put(PutItemRequest.builder()
                          .tableName(getTableName())
                          .item(encode(value))
                          .returnConsumedCapacity(ReturnConsumedCapacity.INDEXES)
                          .build());
        return new ExtendedItemResult<>(putResponse.hasAttributes() ? decode(putResponse.attributes()) : null, putResponse.consumedCapacity());
    }

    public CompletableFuture<T> putAsync(T value) {
        return putAsync(PutItemRequest.builder()
                            .tableName(getTableName())
                            .item(encode(value))
                            .build())
                   .thenApply(resp -> resp.hasAttributes() ? decode(resp.attributes()) : null);
    }

    public CompletableFuture<ExtendedItemResult<T>> putExtendedAsync(T value) {
        return putAsync(PutItemRequest.builder()
                            .tableName(getTableName())
                            .item(encode(value))
                            .returnConsumedCapacity(ReturnConsumedCapacity.INDEXES)
                            .build())
                   .thenApply(resp -> new ExtendedItemResult<>(
                       resp.hasAttributes() ? decode(resp.attributes()) : null, resp.consumedCapacity()));
    }

    private PutItemResponse put(PutItemRequest request) {
        return getClient() == null ? getAsyncClient().putItem(request).join() : getClient().putItem(request);
    }

    private CompletableFuture<PutItemResponse> putAsync(PutItemRequest request) {
        return getAsyncClient() == null ? CompletableFuture.supplyAsync(() -> getClient().putItem(request)) : getAsyncClient().putItem(request);
    }

    public final T delete(T value) {
        return delete(getPartitionKey(value), getSortKey(value));
    }

    public T delete(PartitionT partitionKey, SortT sortKey) {
        var deleteResponse = delete(DeleteItemRequest.builder()
                   .tableName(getTableName())
                   .key(keysToMap(partitionKey, sortKey))
                   .build());
        return deleteResponse.hasAttributes() ? decode(deleteResponse.attributes()) : null;
    }

    public final ExtendedItemResult<T> deleteExtended(T value) {
        return deleteExtended(getPartitionKey(value), getSortKey(value));
    }

    public ExtendedItemResult<T> deleteExtended(PartitionT partitionKey, SortT sortKey) {
        var deleteResponse = delete(DeleteItemRequest.builder()
                                        .tableName(getTableName())
                                        .key(keysToMap(partitionKey, sortKey))
                                        .returnConsumedCapacity(ReturnConsumedCapacity.INDEXES)
                                        .build());
        return new ExtendedItemResult<>(deleteResponse.hasAttributes() ? decode(deleteResponse.attributes()) : null, deleteResponse.consumedCapacity());
    }
    
    public final CompletableFuture<T> deleteAsync(T value) {
        return deleteAsync(getPartitionKey(value), getSortKey(value));
    }

    public CompletableFuture<T> deleteAsync(PartitionT partitionKey, SortT sortKey) {
        return deleteAsync(DeleteItemRequest.builder()
                               .tableName(getTableName())
                               .key(keysToMap(partitionKey, sortKey))
                               .build())
                   .thenApply(resp -> resp.hasAttributes() ? decode(resp.attributes()) : null);
    }

    public final CompletableFuture<ExtendedItemResult<T>> deleteExtendedAsync(T value) {
        return deleteExtendedAsync(getPartitionKey(value), getSortKey(value));
    }

    public CompletableFuture<ExtendedItemResult<T>> deleteExtendedAsync(PartitionT partitionKey, SortT sortKey) {
        return deleteAsync(DeleteItemRequest.builder()
                               .tableName(getTableName())
                               .key(keysToMap(partitionKey, sortKey))
                               .returnConsumedCapacity(ReturnConsumedCapacity.INDEXES)
                               .build())
                   .thenApply(resp -> new ExtendedItemResult<>(
                       resp.hasAttributes() ? decode(resp.attributes()) : null, resp.consumedCapacity()));
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
